import concurrent.futures
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import gcsfs
import pyarrow as pa
import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials as GoogleOAuth2Credentials
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    AuthorizationExpiredError,
    CommitStateUnknownException,
    NoSuchNamespaceError,
    NoSuchTableError,
    OAuthError,
    ServerError,
    ServiceUnavailableError,
    UnauthorizedError,
)

from helpers.catalog_manager import CatalogManager, arrow_to_iceberg_schema

log = logging.getLogger(__name__)

BIGLAKE_CATALOG_URL = "https://biglake.googleapis.com/iceberg/v1beta/restcatalog"
GOOGLE_OAUTH2_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
NAMESPACE_PREFIX = "ch_e2e_bl_"

# Auth-expiry errors are non-retryable everywhere in this manager: self.catalog
# is a RestCatalog built once in __init__ with a static bearer token, and
# _refresh_token_if_needed refreshes only the separate GoogleOAuth2Credentials
# object, never the token baked into the catalog. Retrying any catalog call after
# the token expires just resends the same stale token until the deadline.
# Rebuilding self.catalog mid-retry is unsafe because callers share it
# concurrently (test_many_tables_pagination fans catalog calls across a 10-thread
# pool), so auth expiry is always left to propagate. These mirror pyiceberg's own
# retry-then-reraise set for its REST client (AuthorizationExpiredError,
# UnauthorizedError; OAuthError covers the token-endpoint path).
_AUTH_EXPIRY_ERRORS = (
    AuthorizationExpiredError,
    OAuthError,
    UnauthorizedError,
)

# REST/network errors that create_table retries; pyiceberg's REST client
# retries only auth errors (stop_after_attempt(2)), so these otherwise escape.
# Auth-expiry errors are deliberately excluded (see _AUTH_EXPIRY_ERRORS).
_TRANSIENT_CREATE_ERRORS = (
    ServerError,
    ServiceUnavailableError,
    CommitStateUnknownException,
    requests.exceptions.RequestException,
)

# The only errors that PROVE a table is gone. wait_for_table_gone treats these
# as confirmation; any other error from load_table (5xx, connection reset, the
# transient class create_table retries) is NOT proof of removal and must be
# retried under the deadline rather than accepted as "gone".
_TABLE_GONE_ERRORS = (
    NoSuchTableError,
    NoSuchNamespaceError,
)


@dataclass
class BigLakeConfig:
    project_id: str
    gcs_bucket: str
    gcs_prefix: str
    adc_client_id: str
    adc_client_secret: str
    adc_refresh_token: str


class BigLakeCatalogManager(CatalogManager):
    """Manages table lifecycle inside a BigLake Metastore Iceberg REST catalog.

    All tables for a test session share a single namespace to avoid
    ClickHouse's recursive namespace traversal hitting BigLake's
    flat-namespace limitation (multi-level namespaces unsupported).
    """

    def __init__(self, config: BigLakeConfig):
        self.config = config
        self._credential = GoogleOAuth2Credentials(
            token=None,
            refresh_token=config.adc_refresh_token,
            token_uri=GOOGLE_OAUTH2_TOKEN_ENDPOINT,
            client_id=config.adc_client_id,
            client_secret=config.adc_client_secret,
        )
        self._credential.refresh(GoogleAuthRequest())

        token_expiry_ms = str(
            int(self._credential.expiry.timestamp() * 1000)
        )
        self.catalog = load_catalog(
            "clickhouse_e2e_biglake",
            type="rest",
            uri=BIGLAKE_CATALOG_URL,
            warehouse=self._warehouse_path(),
            token=self._credential.token,
            **{
                "header.x-goog-user-project": config.project_id,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "gcs.project-id": config.project_id,
                "gcs.oauth2.token": self._credential.token,
                "gcs.oauth2.token-expires-at": token_expiry_ms,
            },
        )

        # Encode creation timestamp in the name so stale cleanup can skip
        # namespaces from concurrently running sessions.
        self._session_namespace = (
            f"{NAMESPACE_PREFIX}{int(time.time())}_{uuid.uuid4().hex[:6]}"
        )
        self._tables_created: List[str] = []

        self._cleanup_stale_namespaces()
        self._cleanup_stale_storage_paths()

        self._refresh_token_if_needed()
        self.catalog.create_namespace(self._session_namespace)
        log.info("Created session namespace '%s'", self._session_namespace)

    def _namespace_gcs_path(self, namespace: str) -> str:
        """Build the GCS path for a namespace's data files, handling
        the case where `gcs_prefix` is empty (no leading subdirectory)."""
        if self.config.gcs_prefix:
            return f"{self.config.gcs_bucket}/{self.config.gcs_prefix}/{namespace}"
        return f"{self.config.gcs_bucket}/{namespace}"

    def _cleanup_stale_storage_paths(self) -> None:
        """Remove leftover GCS storage directories from previous BigLake
        test runs.

        BigLake drops namespace records on `drop_namespace`, but the
        underlying GCS data files are NOT auto-purged. Combined with a
        long-standing path-join bug (when `gcs_prefix=""` the previous
        cleanup attempted to delete `bucket//<ns>`, an invalid path),
        the bucket has accumulated thousands of orphan storage prefixes.

        We list `ch_e2e_bl_*` directories at the configured storage root
        and delete those older than `_STALE_GRACE_SECONDS`. Age is
        determined either from the encoded timestamp in the directory
        name (`ch_e2e_bl_<ts>_<hex>`) or, for the legacy
        `ch_e2e_bl_<hex>` form, by the `updated` time of the first
        object found inside.
        """
        try:
            self._refresh_token_if_needed()
            fs = gcsfs.GCSFileSystem(token=self._credential.token)
            root = (
                f"{self.config.gcs_bucket}/{self.config.gcs_prefix}"
                if self.config.gcs_prefix
                else self.config.gcs_bucket
            )
            now = datetime.now(timezone.utc)
            stale: List[str] = []
            for entry in fs.ls(root, detail=True):
                name = entry.get("name", "").rstrip("/")
                tail = name.rsplit("/", 1)[-1]
                if not tail.startswith(NAMESPACE_PREFIX):
                    continue
                if tail == self._session_namespace:
                    continue
                age_seconds = self._estimate_storage_age(fs, name, tail, now)
                if age_seconds is None:
                    continue
                if age_seconds >= self._STALE_GRACE_SECONDS:
                    stale.append(name)
            if not stale:
                return
            log.info(
                "Cleaning up %d stale BigLake storage paths (older than %ds)",
                len(stale), self._STALE_GRACE_SECONDS,
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                list(pool.map(self._delete_gcs_prefix, stale))
        except Exception as exc:
            log.warning("Stale BigLake storage cleanup failed: %s", exc)

    def _estimate_storage_age(
        self, fs: gcsfs.GCSFileSystem, full_path: str, tail: str, now: datetime,
    ) -> Optional[float]:
        """Return the age (in seconds) of a `ch_e2e_bl_*` storage prefix.

        New-format directory names embed the session creation timestamp
        (`ch_e2e_bl_<ts>_<hex>`), so the age comes straight from the
        name.  For the legacy `ch_e2e_bl_<hex>` form (pre-timestamp
        naming) we walk into the nested `tbl_*/data/...` and
        `tbl_*/metadata/...` files and use the first `updated`
        timestamp we can read.  If no probe-able file is found we
        return `None` so the caller skips the prefix rather than
        assuming it is stale -- that prevents a concurrent test
        session still on the old naming format from losing a fresh
        storage path it just created."""
        parts = tail.split("_")
        try:
            ns_ts = int(parts[3])
            return (now - datetime.fromtimestamp(ns_ts, tz=timezone.utc)).total_seconds()
        except (IndexError, ValueError):
            pass
        try:
            for tbl_entry in fs.ls(full_path, detail=True):
                if tbl_entry.get("type") != "directory":
                    continue
                for sub_entry in fs.ls(tbl_entry["name"], detail=True):
                    if sub_entry.get("type") != "directory":
                        continue
                    for obj in fs.ls(sub_entry["name"], detail=True):
                        if obj.get("type") != "file":
                            continue
                        updated = obj.get("updated")
                        if not updated:
                            continue
                        if isinstance(updated, str):
                            updated = datetime.fromisoformat(
                                updated.replace("Z", "+00:00")
                            )
                        elif updated.tzinfo is None:
                            updated = updated.replace(tzinfo=timezone.utc)
                        return (now - updated).total_seconds()
        except Exception as exc:
            log.debug("age probe of '%s' failed: %s", full_path, exc)
        return None

    def _warehouse_path(self) -> str:
        if self.config.gcs_prefix:
            return f"gs://{self.config.gcs_bucket}/{self.config.gcs_prefix}"
        return f"gs://{self.config.gcs_bucket}"

    def _refresh_token_if_needed(self) -> str:
        if not self._credential.valid:
            self._credential.refresh(GoogleAuthRequest())
        return self._credential.token

    # Only clean up namespaces older than this to avoid racing with other
    # concurrently running test sessions.
    _STALE_GRACE_SECONDS: int = 43200  # 12 hours

    def _cleanup_stale_namespaces(self) -> None:
        """Remove leftover namespaces from previous test runs."""
        try:
            self._refresh_token_if_needed()
            now = time.time()
            namespaces = self.catalog.list_namespaces()
            for ns_tuple in namespaces:
                ns = ns_tuple[0] if isinstance(ns_tuple, tuple) else ns_tuple
                if not ns.startswith(NAMESPACE_PREFIX):
                    continue
                if ns == self._session_namespace:
                    continue
                # Skip namespaces that may belong to a concurrent session:
                # parse the timestamp encoded as ch_e2e_bl_{ts}_{hex}.
                parts = ns.split("_")
                try:
                    ns_ts = int(parts[3])
                    if now - ns_ts < self._STALE_GRACE_SECONDS:
                        log.debug(
                            "Skipping recently-created namespace '%s'", ns
                        )
                        continue
                except (IndexError, ValueError):
                    pass  # Old format without timestamp — proceed with cleanup
                try:
                    for tbl in self.catalog.list_tables(ns):
                        tbl_id = f"{ns}.{tbl[1]}"
                        try:
                            self.catalog.drop_table(tbl_id)
                        except Exception:
                            pass
                    self.catalog.drop_namespace(ns)
                    log.info("Cleaned up stale namespace '%s'", ns)
                    self._delete_gcs_prefix(self._namespace_gcs_path(ns))
                except Exception as exc:
                    log.warning("Failed to clean up namespace '%s': %s", ns, exc)
        except Exception as exc:
            log.warning("Stale namespace cleanup failed: %s", exc)

    @classmethod
    def from_env(cls) -> "BigLakeCatalogManager":
        project_id = os.getenv("E2E_GCP_PROJECT", "") or os.getenv("E2E_BIGLAKE_PROJECT_ID", "")
        gcs_bucket = os.getenv("E2E_GCP_BUCKET", "") or os.getenv("E2E_BIGLAKE_GCS_BUCKET", "")
        gcs_prefix = os.getenv("E2E_BIGLAKE_GCS_PREFIX", "clickhouse-e2e-biglake")
        adc_client_id = os.getenv("E2E_BIGLAKE_ADC_CLIENT_ID", "")
        adc_client_secret = os.getenv("E2E_BIGLAKE_ADC_CLIENT_SECRET", "")
        adc_refresh_token = os.getenv("E2E_BIGLAKE_ADC_REFRESH_TOKEN", "")

        required = {
            "E2E_GCP_PROJECT": project_id,
            "E2E_GCP_BUCKET": gcs_bucket,
            "E2E_BIGLAKE_ADC_CLIENT_ID": adc_client_id,
            "E2E_BIGLAKE_ADC_CLIENT_SECRET": adc_client_secret,
            "E2E_BIGLAKE_ADC_REFRESH_TOKEN": adc_refresh_token,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise Exception("Missing BigLake e2e settings: " + ", ".join(missing))

        return cls(
            BigLakeConfig(
                project_id=project_id,
                gcs_bucket=gcs_bucket,
                gcs_prefix=gcs_prefix,
                adc_client_id=adc_client_id,
                adc_client_secret=adc_client_secret,
                adc_refresh_token=adc_refresh_token,
            )
        )

    # ------------------------------------------------------------------
    # CatalogManager interface
    # ------------------------------------------------------------------

    @staticmethod
    def make_database_name() -> str:
        return f"e2e_biglake_{uuid.uuid4().hex[:8]}"

    def create_table(
        self,
        data: pa.Table,
        table_name: Optional[str] = None,
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> str:
        # pyiceberg's REST client only retries auth errors and gives up
        # after 2 attempts, so a single transient BigLake blip (5xx,
        # connection reset, commit-state-unknown) otherwise propagates and
        # fails the shared module-scoped fixture. Retry create+append under a
        # wall-clock deadline, mirroring wait_for_table_ready/resolve_table_name.
        #
        # Retrying is safe ONLY for the auto-generated-name path: each attempt
        # uses a fresh unique name, so a partial prior attempt (table created,
        # append failed) cannot resurface as TableAlreadyExistsError nor
        # duplicate rows -- creation stays exactly-once. When a caller supplies
        # a fixed table_name (test_many_tables_pagination asserts the exact
        # short names it passed appear in SHOW TABLES, so we cannot substitute a
        # UUID), a fresh name is not an option, so that path makes a single
        # attempt and lets a transient error propagate as before.
        namespace = self._session_namespace
        iceberg_schema = arrow_to_iceberg_schema(data)

        if table_name is not None:
            self._refresh_token_if_needed()
            try:
                table = self.catalog.create_table(
                    identifier=f"{namespace}.{table_name}",
                    schema=iceberg_schema,
                    location=f"{self._warehouse_path()}/{namespace}/{table_name}",
                )
                table.append(data)
            except Exception:
                # create may have committed before a later step (append, or a
                # CommitStateUnknownException from create itself) threw.
                # Reconcile the partial attempt so no table is leaked untracked,
                # then propagate: named creates are single-attempt, so the
                # caller-supplied identifier is never created twice.
                self._drop_failed_attempt(table_name)
                raise
            log.info("Created BigLake Iceberg table '%s.%s'", namespace, table_name)
            self._tables_created.append(table_name)
            return table_name

        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            candidate = f"tbl_{uuid.uuid4().hex[:8]}"
            table_identifier = f"{namespace}.{candidate}"
            table_location = f"{self._warehouse_path()}/{namespace}/{candidate}"

            self._refresh_token_if_needed()
            try:
                table = self.catalog.create_table(
                    identifier=table_identifier,
                    schema=iceberg_schema,
                    location=table_location,
                )
                table.append(data)
            except Exception as exc:
                # ANY failure here may have left a committed table behind
                # (create succeeds before append can throw; create itself can
                # raise CommitStateUnknownException after committing). Reconcile
                # the partial attempt FIRST -- for every exception, transient or
                # not -- so nothing is leaked untracked, then decide retry vs
                # re-raise.
                self._drop_failed_attempt(candidate)
                if not isinstance(exc, _TRANSIENT_CREATE_ERRORS):
                    raise
                if time.monotonic() >= deadline:
                    raise
                log.warning(
                    "create_table('%s') transient failure (attempt %d), "
                    "retrying: %s",
                    table_identifier,
                    attempt,
                    exc,
                )
                time.sleep(poll_interval)
                continue

            log.info(
                "Created BigLake Iceberg table '%s' at %s (attempt %d)",
                table_identifier,
                table_location,
                attempt,
            )
            self._tables_created.append(candidate)
            return candidate

    def _drop_failed_attempt(self, table_name: str) -> None:
        """Reconcile a table left behind by a failed create attempt.

        Drops it if possible; otherwise registers it in `_tables_created` so the
        end-of-session `cleanup_all` sweep reaps it. Never leaves a possibly-live
        table untracked. `NoSuchTableError` is treated as unconfirmed (not proof
        the table is gone): BigLake indexes asynchronously, so a just-committed
        table can be invisible to `drop_table` here and only surface later. The
        only case that skips registration is a clearly successful drop.
        """
        identifier = f"{self._session_namespace}.{table_name}"
        try:
            self._refresh_token_if_needed()
            self.catalog.drop_table(identifier)
            log.info("Dropped partially-created table '%s'", identifier)
            return
        except Exception as exc:
            log.warning(
                "Could not confirm drop of partially-created table '%s' (%s); "
                "tracking it for end-of-session cleanup",
                identifier,
                exc,
            )
        if table_name not in self._tables_created:
            self._tables_created.append(table_name)

    def wait_for_table_ready(
        self,
        table_name: str,
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> None:
        """Poll until the table is both loadable AND visible in
        BigLake's namespace listing.
        """
        namespace = self._session_namespace
        identifier = f"{namespace}.{table_name}"
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            self._refresh_token_if_needed()
            try:
                self.catalog.load_table(identifier)
                listed = self.catalog.list_tables(namespace)
                listed_names = [t[1] for t in listed]
                if table_name in listed_names:
                    log.info(
                        "Table '%s' ready and listed after %d attempts",
                        identifier,
                        attempt,
                    )
                    return
                log.warning(
                    "Table '%s' loadable but not yet listed "
                    "(attempt %d, listed=%s)",
                    identifier,
                    attempt,
                    listed_names,
                )
            except _AUTH_EXPIRY_ERRORS:
                # Non-retryable (see _AUTH_EXPIRY_ERRORS): the stale static token
                # cannot be refreshed in place, so polling would only burn the
                # deadline. Propagate, mirroring create_table/cleanup_all.
                raise
            except Exception as exc:
                log.warning(
                    "load_table('%s') failed (attempt %d): %s",
                    identifier,
                    attempt,
                    exc,
                )

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Table '{identifier}' not ready/listed via pyiceberg "
                    f"after {timeout}s ({attempt} attempts)."
                )
            time.sleep(poll_interval)

    def wait_for_table_gone(
        self,
        table_name: str,
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> None:
        """Poll until load_table proves the table is gone.

        Only a genuine not-found (see `_TABLE_GONE_ERRORS`) confirms removal.
        A transient load failure -- the same 5xx / connection-reset class
        create_table retries -- does NOT prove the table is gone, so it is
        retried under the deadline like a still-visible table rather than
        accepted as success. Otherwise a teardown-time blip on load_table would
        let test_table_disappears_after_cleanup pass with a live table behind.
        Auth-expiry is non-retryable (see `_AUTH_EXPIRY_ERRORS`) and likewise is
        not proof of removal, so it propagates.
        """
        namespace = self._session_namespace
        identifier = f"{namespace}.{table_name}"
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            self._refresh_token_if_needed()
            try:
                self.catalog.load_table(identifier)
                log.info(
                    "Table '%s' still visible (attempt %d)",
                    identifier,
                    attempt,
                )
            except _AUTH_EXPIRY_ERRORS:
                # Must precede the handlers below: an expired static token is
                # non-retryable (see _AUTH_EXPIRY_ERRORS) and, crucially, is NOT
                # proof the table is gone. Treating it as "gone" here would be a
                # false-positive cleanup, so propagate instead of returning.
                raise
            except _TABLE_GONE_ERRORS:
                log.info(
                    "Table '%s' gone after %d attempts",
                    identifier,
                    attempt,
                )
                return
            except Exception as exc:
                # A transient load failure (5xx, connection reset) is NOT proof
                # the table is gone. Retry under the deadline instead of
                # accepting a false-positive cleanup.
                log.warning(
                    "load_table('%s') failed transiently (attempt %d): %s; "
                    "retrying, not treating as gone",
                    identifier,
                    attempt,
                    exc,
                )

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Table '{identifier}' still visible via pyiceberg "
                    f"after {timeout}s ({attempt} attempts)."
                )
            time.sleep(poll_interval)

    def cleanup_table(self, table_name: str) -> None:
        self._refresh_token_if_needed()
        namespace = self._session_namespace
        table_identifier = f"{namespace}.{table_name}"
        try:
            self.catalog.drop_table(table_identifier)
            log.info("Dropped table '%s'", table_identifier)
            # Confirmed gone -- stop tracking so the end-of-session cleanup_all
            # sweep does not re-drop (and pointlessly retry) an already-removed
            # table. A failed drop leaves it tracked so cleanup_all retries it.
            if table_name in self._tables_created:
                self._tables_created.remove(table_name)
        except Exception as exc:
            log.warning("Cleanup of table '%s' failed: %s", table_identifier, exc)

    def _delete_gcs_prefix(self, gcs_path: str) -> None:
        try:
            fs = gcsfs.GCSFileSystem(token=self._credential.token)
            if fs.exists(gcs_path):
                fs.rm(gcs_path, recursive=True)
                log.info("Deleted GCS path '%s'", gcs_path)
        except Exception as exc:
            log.warning("GCS cleanup of '%s' failed: %s", gcs_path, exc)

    def _drop_confirmed(self, table_name: str) -> bool:
        """Attempt to drop a tracked table; return True ONLY on a confirmed
        drop (the catalog `drop_table` call returned without raising).

        Any raised drop returns False so the caller retains the name and
        retries. We deliberately do NOT treat a raised drop as "gone" -- a
        transient 5xx/network error obviously leaves the table live, and a
        `NoSuchTableError` does NOT prove removal either: under BigLake
        async-index lag a just-committed table can be briefly invisible to
        `drop_table` and surface later. Forgetting a name on that ambiguous
        signal is exactly the leak this path guards against, so "confirmed
        gone" means only a `drop_table` that returned.

        Auth-expiry errors (see `_AUTH_EXPIRY_ERRORS`) are re-raised, not
        retained-and-retried: `self.catalog` holds a static token that
        `_refresh_token_if_needed` cannot push into it, so every retry resends
        the same stale token. `cleanup_all` turns that into a fail-fast exit.
        """
        identifier = f"{self._session_namespace}.{table_name}"
        self._refresh_token_if_needed()
        try:
            self.catalog.drop_table(identifier)
            log.info("Dropped tracked table '%s'", identifier)
            return True
        except _AUTH_EXPIRY_ERRORS:
            raise
        except Exception as drop_exc:
            log.warning(
                "Drop of tracked table '%s' unconfirmed (%s); retaining",
                identifier,
                drop_exc,
            )
            return False

    def cleanup_all(self, timeout: float = 120, poll_interval: float = 5) -> None:
        """Drop every tracked table, then the session namespace and its GCS
        storage.

        A tracked table is forgotten ONLY once its drop is confirmed (see
        `_drop_confirmed`). Drops that raise -- transient 5xx/network errors,
        or BigLake async-index lag that makes a just-committed table briefly
        invisible to `drop_table` -- are retried under a bounded wall-clock
        deadline rather than being cleared unconditionally, so a teardown-time
        blip cannot lose track of a still-live table. Any tables still
        unconfirmed after the deadline are retained (never cleared blindly) and
        logged; the final namespace drop + GCS prefix delete are the storage
        backstop for anything the per-table catalog drop could not reach.

        Auth-expiry is the one non-retryable case: since `self.catalog` cannot
        pick up a refreshed token, retrying drops would only burn the deadline
        and still leave the tables behind. On auth expiry we stop immediately
        (no deadline loop), keep the tables tracked, run the GCS storage
        backstop -- which builds a fresh gcsfs client from the refreshed
        credential and so still reaps the data files -- and then propagate,
        mirroring create_table.
        """
        deadline = time.monotonic() + timeout
        pending = list(self._tables_created)
        try:
            while pending:
                pending = [n for n in pending if not self._drop_confirmed(n)]
                if not pending or time.monotonic() >= deadline:
                    break
                time.sleep(poll_interval)
        except _AUTH_EXPIRY_ERRORS as auth_exc:
            # Fail fast: the static catalog token has expired and cannot be
            # refreshed in place, so every further drop would resend it. Keep
            # the tables tracked (never cleared) and fall back to the GCS
            # storage sweep, which uses the refreshed credential, then
            # propagate so the teardown failure is visible.
            log.warning(
                "cleanup_all: auth expired mid-cleanup (%s); skipping catalog "
                "drops and relying on the GCS storage sweep for tracked "
                "table(s): %s",
                auth_exc,
                pending,
            )
            self._delete_gcs_prefix(
                self._namespace_gcs_path(self._session_namespace)
            )
            raise
        if pending:
            log.warning(
                "cleanup_all: %d tracked table(s) could not be confirmed "
                "dropped after %ds; relying on the namespace/storage sweep: %s",
                len(pending),
                timeout,
                pending,
            )
        # Keep only the names we could NOT confirm gone -- never clear blindly.
        self._tables_created = pending

        self._refresh_token_if_needed()
        try:
            self.catalog.drop_namespace(self._session_namespace)
            log.info("Dropped session namespace '%s'", self._session_namespace)
        except _AUTH_EXPIRY_ERRORS as auth_exc:
            # Same non-retryable contract as the per-table loop: the static
            # catalog token cannot be refreshed in place. When pending is empty
            # (the common case, since cleanup_table untracks confirmed drops) or
            # the token expires only after the loop, this final drop_namespace
            # is the one remaining catalog call, and swallowing an auth failure
            # here would silently leave the namespace behind. Run the GCS
            # storage backstop (fresh gcsfs from the refreshed credential), then
            # propagate so the teardown failure is visible.
            log.warning(
                "cleanup_all: auth expired dropping namespace '%s' (%s); "
                "relying on the GCS storage sweep",
                self._session_namespace,
                auth_exc,
            )
            self._delete_gcs_prefix(
                self._namespace_gcs_path(self._session_namespace)
            )
            raise
        except Exception:
            # Best-effort: the namespace may be non-empty (retained tables) or
            # already gone. The GCS prefix delete below is the storage backstop.
            pass

        self._delete_gcs_prefix(self._namespace_gcs_path(self._session_namespace))

    def resolve_table_name(
        self,
        node,
        database_name: str,
        short_name: str,
        timeout: float = 60,
        poll_interval: float = 3,
    ) -> str:
        """Retry SHOW TABLES until the table appears.

        BigLake has a propagation delay between table creation and
        ClickHouse's SHOW TABLES seeing it via the REST API.
        """
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            raw = node.query(f"SHOW TABLES FROM {database_name}").strip()
            for line in raw.splitlines():
                if short_name in line:
                    log.info(
                        "Resolved '%s' in '%s' after %d attempts",
                        short_name,
                        database_name,
                        attempt,
                    )
                    return line.strip()

            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Table '{short_name}' not found in "
                    f"SHOW TABLES output after {timeout}s "
                    f"({attempt} attempts):\n{raw}"
                )
            log.warning(
                "SHOW TABLES FROM %s: '%s' not visible (attempt %d)",
                database_name,
                short_name,
                attempt,
            )
            time.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def clickhouse_env_variables(self) -> Dict[str, str]:
        return {}

    def create_catalog(self, node, database_name: str) -> None:
        cfg = self.config
        node.query(
            f"""
DROP DATABASE IF EXISTS {database_name};
SET allow_experimental_database_iceberg=1;
CREATE DATABASE {database_name} ENGINE = DataLakeCatalog('{BIGLAKE_CATALOG_URL}')
SETTINGS
    catalog_type='biglake',
    warehouse='{self._warehouse_path()}',
    google_project_id='{cfg.project_id}',
    google_adc_client_id='{cfg.adc_client_id}',
    google_adc_client_secret='{cfg.adc_client_secret}',
    google_adc_refresh_token='{cfg.adc_refresh_token}'
"""
        )
