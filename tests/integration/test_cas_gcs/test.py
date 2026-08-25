"""CAS over GCS, end to end against a deterministic fake GCS XML service.

What this suite is for: the unit tests prove that a request marked `NativeConditional` gets
generation semantics and a `Default` one does not. They cannot prove that a real CAS mount, driving
the production capability battery over a real HTTP client, still works once generation semantics stop
being applied to every request on the client. That is what runs here.

The fake service (`gcs_mocks/server.py`) models generations and ETags as two disjoint domains, so no
assertion below can pass by accident on a value that would serve as either token. Its hostname
contains no `storage.googleapis.com`, so a mount that selects generation tokens proves the capability
came from the explicit `http_client` value.

Marking is observable on the wire on the OAuth path, by absence rather than presence: marking a
request deletes `x-amz-api-version`, so a `Default` request still carries it. That is what lets this
suite assert per-request marking on a single client rather than only per disk. It does not hold on
`gcs_hmac`, where the header is stripped from every request regardless of mode.

The second half of this file is adversarial. Those tests drive the fake into modes no correct client
provokes — a service that silently ignores a generation placed in the ETag domain, a successful write
whose response carries no generation — and one of them remounts the whole node against the permissive
service. They run after the observational tests because they add rows to the capture log the tests
above read, and because two of them restart the server.

Not covered here, deliberately: CAS object attributes. The attribute parameter is plumbed through the
CAS write paths but no production caller fills it — every call site uses the `Backend` overloads that
forward an empty `ObjectMeta` — so an attribute round trip driven from SQL would be asserting the
fake's own behaviour. The prefix mapping is covered by the dialect and client unit tests.
"""

import json
import os
import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

GCS_HOST = "fakegcs"
GCS_PORT = 8080
METADATA_HOST = "metadata.google.internal"
METADATA_PORT = 80

# One CAS disk per supported `http_client` value, plus an ordinary non-CAS disk on the same fake
# service. Each has its own bucket, so the capture log partitions by bucket with no ambiguity.
CAS_DISKS = {"cas_gcs_oauth": "oauthbucket", "cas_gcs_hmac": "hmacbucket"}
PLAIN_DISK = "plain_gcs_oauth"
PLAIN_BUCKET = "plainbucket"

NUM_ROWS = 200

# Where the fixture installs the disk configuration, so a test can rewrite it and reload.
CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/cas_gcs.xml"

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    # The storage configuration is deliberately NOT passed as a main config: a CAS disk mounts at
    # server startup and runs its capability battery against the store, and there is no fallback when
    # the store is unreachable. The mock services can only be launched after their containers are up,
    # which is after this node's server has already started, so the config is installed and the server
    # restarted once the fake service answers.
    cluster.add_instance(
        "node",
        stay_alive=True,
    )
    cluster.add_instance(
        GCS_HOST,
        hostname=GCS_HOST,
        image="altinityinfra/python-bottle",
        tag="latest",
        stay_alive=True,
    )
    # The fake GCE metadata server takes the real default hostname `metadata.google.internal` on the
    # test's docker network, so no `metadata_service` override is needed. A CAS disk would ACCEPT one —
    # `non_cas_keys` in ContentAddressedSettings.cpp lists `metadata_service`, `request_token_path`,
    # `service_account` and the ADC triple — so this is a fixture simplification, not a constraint.
    cluster.add_instance(
        METADATA_HOST,
        hostname=METADATA_HOST,
        image="altinityinfra/python-bottle",
        tag="latest",
        stay_alive=True,
    )

    try:
        cluster.start()
        script_dir = os.path.join(os.path.dirname(__file__), "gcs_mocks")
        start_mock_servers(
            cluster,
            script_dir,
            [
                ("server.py", GCS_HOST, str(GCS_PORT)),
                ("auth.py", METADATA_HOST, str(METADATA_PORT)),
            ],
        )
        node = cluster.instances["node"]
        node.copy_file_to_container(
            os.path.join(os.path.dirname(__file__), "configs", "config.xml"),
            CONFIG_IN_CONTAINER,
        )
        node.restart_clickhouse()

        for disk in CAS_DISKS:
            _create_and_fill(node, disk)
        _create_and_fill(node, PLAIN_DISK)
        yield cluster
    finally:
        cluster.shutdown()


def _create_and_fill(node, disk):
    table = "t_" + disk
    node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    node.query(
        """
        CREATE TABLE {} (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = '{}'
        """.format(
            table, disk
        )
    )
    node.query(
        "INSERT INTO {} SELECT number, toString(number) FROM numbers({})".format(
            table, NUM_ROWS
        )
    )


def _control(path):
    container = cluster.get_container_id(GCS_HOST)
    raw = cluster.exec_in_container(
        container, ["curl", "-sS", "http://localhost:{}{}".format(GCS_PORT, path)]
    )
    return json.loads(raw)


def _control_post(path):
    container = cluster.get_container_id(GCS_HOST)
    raw = cluster.exec_in_container(
        container,
        ["curl", "-sS", "-X", "POST", "http://localhost:{}{}".format(GCS_PORT, path)],
    )
    return json.loads(raw) if raw.strip().startswith(("{", "[")) else raw


def _counters():
    return _control("/_control/counters")


def _set_if_match_mode(mode):
    return _control_post("/_control/mode?if_match=" + mode)


def _set_omit_generation(enabled):
    return _control_post("/_control/mode?omit_generation=" + ("1" if enabled else "0"))


def _raw(method, path, headers=()):
    """Issue one request to the fake from inside its own container and return its status code.

    Used only to drive request shapes production never sends, so that the fake's own discriminating
    power can be asserted rather than assumed.

    HEAD goes through `--head` rather than `-X HEAD`: with `-X HEAD` curl sends the request but still
    waits for a response body, and a HEAD reply never has one, so it blocks until something kills it.
    `--max-time` is here for the same class of mistake — a fixture hang should cost seconds, not the
    module's whole budget.
    """
    container = cluster.get_container_id(GCS_HOST)
    command = ["curl", "-sS", "--max-time", "30", "-o", "/dev/null", "-w", "%{http_code}"]
    command += ["--head"] if method == "HEAD" else ["-X", method]
    for name, value in headers:
        command += ["-H", "{}: {}".format(name, value)]
    command.append("http://localhost:{}{}".format(GCS_PORT, path))
    return int(cluster.exec_in_container(container, command).strip())


def _token_fetches():
    container = cluster.get_container_id(METADATA_HOST)
    raw = cluster.exec_in_container(
        container,
        ["curl", "-sS", "http://localhost:{}/_control/tokens".format(METADATA_PORT)],
    )
    return json.loads(raw)["fetches"]


def _reset_token_fetches():
    container = cluster.get_container_id(METADATA_HOST)
    cluster.exec_in_container(
        container,
        ["curl", "-sS", "http://localhost:{}/_control/tokens/reset".format(METADATA_PORT)],
    )


def _captured(bucket=None):
    records = _control("/_control/requests")
    if bucket is None:
        return records
    return [r for r in records if r["bucket"] == bucket]


def _minted():
    return _control("/_control/minted")


def _next_seq():
    """The `seq` the fake's next captured request will carry, so a later slice can start here."""
    return len(_control("/_control/requests"))


def _captured_since(seq, bucket=None):
    return [r for r in _captured(bucket) if r["seq"] >= seq]


def _unquote(value):
    return value.strip().strip('"')


def _generation_preconditions(records):
    return [
        r["headers"]["x-goog-if-generation-match"]
        for r in records
        if "x-goog-if-generation-match" in r["headers"]
    ]


def _has_goog_metadata(record):
    return any(name.startswith("x-goog-meta-") for name in record["headers"])


def _is_translated(record):
    return "x-goog-if-generation-match" in record["headers"] or _has_goog_metadata(record)


# `AWS_HEADERS_CLEARED_BEFORE_GCS_AUTHENTICATION` in GCSConditionalDialect.cpp lists
# `x-amz-api-version`, and `prepareGcsRequestForOAuthAuthentication` — which runs ONLY for a marked
# request on the OAuth client — deletes every header in it. So on a `gcp_oauth` client the header's
# PRESENCE means the request was `Default` and its ABSENCE means the request was marked. That makes
# marking observable on the wire for the request kinds the SDK stamps with it, which measurement says
# are GET, HEAD and DELETE but not PUT.
#
# This does NOT hold on `gcs_hmac`: `prepareGcsRequestForGoog4Authentication` runs for every request
# that client sends, marked or not, so the header is always absent there and says nothing about mode.
#
# One other thing deletes the same header, and understanding why it does not fire here is what makes
# the discriminator trustworthy. `Client::BuildHttpRequest` also drops `x-amz-api-version` when
# `api_mode == ApiMode::GCS`, for every request and before the marking logic runs. `api_mode` becomes
# GCS only inside a block gated on `provider_type == ProviderType::GCS`, and `deduceProviderType` is
# pure endpoint-substring matching: GCS requires `storage.googleapis.com` in the URL. This fixture's
# endpoint deliberately contains no such substring, so `provider_type` is UNKNOWN, that block never
# runs, and the header survives to become a marking signal.
#
# Note what this does NOT mean. It is not that these disks have credentials: `gcp_oauth` deliberately
# builds an EMPTY credentials provider chain ("we don't provide any credentials to avoid signing" in
# Credentials.cpp), which is also why no SigV4 artifact such as `x-amz-content-sha256` ever appears.
# Against a real `storage.googleapis.com` endpoint `provider_type` WOULD be GCS, those empty
# credentials would select `ApiMode::GCS`, and the header would be stripped from every request. So
# this discriminator is an artifact of the fixture's non-GCS hostname and could not be reproduced
# against production GCS. Marking itself is unaffected — it depends on `http_client`, not the endpoint
# — so the test still fences the behaviour; only the ability to OBSERVE it is endpoint-dependent.
#
# Consequence for a future reader: if these assertions ever start failing uniformly rather than for
# one request, suspect that the endpoint or `provider_type` changed and the discriminator is gone,
# before suspecting that marking broke.
_MARKING_OBSERVABLE_METHODS = ("GET", "HEAD", "DELETE")


def _looks_default_on_oauth(record):
    return "x-amz-api-version" in record["headers"]


def test_data_is_readable_on_every_disk():
    """Mount, write and read back on both `http_client` values and on the ordinary disk.

    A CAS mount runs `runCapabilityProbe` against the store and refuses the mount unless conditional
    create, conditional overwrite, wrong-token delete rejection and correct-token delete all behave.
    Reaching a correct SELECT therefore proves the whole battery passed over GCS generation
    semantics. Would fail if: the request-mode plumbing stopped marking any CAS operation, since the
    fake rejects a generation sent as an ETag and an ETag sent as a generation.
    """
    node = cluster.instances["node"]
    expected_sum = (NUM_ROWS - 1) * NUM_ROWS // 2
    for disk in list(CAS_DISKS) + [PLAIN_DISK]:
        table = "t_" + disk
        assert int(node.query("SELECT count() FROM {}".format(table))) == NUM_ROWS
        assert int(node.query("SELECT sum(id) FROM {}".format(table))) == expected_sum


def test_fake_service_keeps_the_two_token_domains_disjoint():
    """The fixture's own invariant, asserted rather than assumed.

    Negative control: nothing in ClickHouse can flip this — it is a property of the fake. It is
    asserted anyway because every assertion below is only meaningful while it holds.
    """
    minted = _minted()
    assert minted["generations"], "the fake minted no generation, so nothing below is meaningful"
    assert minted["etags"], "the fake minted no ETag, so nothing below is meaningful"
    for generation in minted["generations"]:
        assert re.fullmatch(r"[0-9]{16}", generation), generation
    for etag in minted["etags"]:
        assert not _unquote(etag).isdigit(), etag
    assert not (set(minted["generations"]) & {_unquote(e) for e in minted["etags"]})


@pytest.mark.parametrize("disk", sorted(CAS_DISKS))
def test_cas_conditional_ops_use_generation_preconditions(disk):
    """Create-if-absent and compare-and-set overwrite both travel as generation preconditions.

    Would fail if: `Client::BuildHttpRequest` stopped copying the mode, or CAS stopped marking its
    conditional writes — the preconditions would then arrive as ETag-valued `If-Match` /
    `If-None-Match` instead, and both value-domain assertions would break.

    The accepted-versus-rejected split matters. An earlier version of this test required EVERY
    precondition to name a minted generation, and it failed against correct behaviour: the capability
    battery fabricates known-wrong tokens on purpose, so a precondition the service never minted is
    expected as long as the service refused it.
    """
    records = _captured(CAS_DISKS[disk])
    assert records, "no request reached the fake for disk {}".format(disk)

    conditional = [r for r in records if "x-goog-if-generation-match" in r["headers"]]
    preconditions = [r["headers"]["x-goog-if-generation-match"] for r in conditional]
    assert "0" in preconditions, "no create-if-absent precondition was sent"
    assert [p for p in preconditions if p != "0"], "no compare-and-set precondition was sent"

    minted = _minted()
    known = set(minted["generations"]) | {"0"}
    etag_values = {_unquote(e) for e in minted["etags"]}

    for record in conditional:
        value = record["headers"]["x-goog-if-generation-match"]
        # Always: the value lives in the generation domain and never in the ETag domain. This is the
        # cross-domain check the whole fixture exists for.
        assert value.isdigit(), "a non-numeric value reached the generation domain: {!r}".format(value)
        assert value not in etag_values, "an ETag was sent as a generation: {}".format(value)

        # The capability battery deliberately fabricates wrong tokens (`900000000000000001` and
        # friends in `CasProbe`) to prove the store enforces preconditions, so a precondition the
        # service never minted is expected — but ONLY if the service rejected it. An ACCEPTED
        # precondition must name a real generation, which is the half that would break if the token
        # plumbing regressed.
        if record["status"] < 300:
            assert value in known, "the service accepted a precondition it never minted: {}".format(value)
        else:
            assert record["status"] == 412, (
                "a conditional request failed with {} rather than a precondition failure".format(
                    record["status"]
                )
            )


@pytest.mark.parametrize("disk", sorted(CAS_DISKS))
def test_no_cas_request_sends_a_generation_as_an_etag(disk):
    """The exact-delete safety invariant, stated over the whole captured run.

    A numeric generation placed in an ETag-valued `If-Match` is the failure mode the design calls
    safety-critical: the design does not assume whether GCS would reject, compare or ignore it.
    Would fail if: any CAS conditional operation lost its mode, since CAS token values are
    generations here.
    """
    generations = set(_minted()["generations"])
    for record in _captured(CAS_DISKS[disk]):
        for header in ("if-match", "if-none-match"):
            value = record["headers"].get(header)
            if value is None:
                continue
            assert _unquote(value) not in generations, (
                "{} {} sent a generation in {}: {}".format(
                    record["method"], record["key"], header, value
                )
            )


@pytest.mark.parametrize("disk", sorted(CAS_DISKS))
def test_stale_exact_delete_preserves_the_object_and_a_matching_one_removes_it(disk):
    """The capability battery's delete pair, read off the wire.

    `runCapabilityProbe` deletes with a known-wrong generation, requires the object to survive, then
    deletes with the correct one. Both halves must be visible, on one key, in that order.
    Would fail if: the wrong-token DELETE were honoured (no 412 would appear), or the correct-token
    DELETE were refused (mount would fail before this test ran).
    """
    records = _captured(CAS_DISKS[disk])
    deletes = [
        r
        for r in records
        if r["method"] == "DELETE" and "x-goog-if-generation-match" in r["headers"]
    ]
    assert deletes, "no generation-conditioned DELETE was sent"

    rejected = [r for r in deletes if r["status"] == 412]
    accepted = [r for r in deletes if r["status"] == 204]
    assert rejected, "no DELETE was rejected on a stale generation"
    assert accepted, "no DELETE was accepted on a matching generation"

    keys_with_both = set(r["key"] for r in rejected) & set(r["key"] for r in accepted)
    assert keys_with_both, "no single key saw both a rejected and an accepted exact DELETE"

    key = sorted(keys_with_both)[0]
    first_rejection = min(r["seq"] for r in rejected if r["key"] == key)
    later_success = min(r["seq"] for r in accepted if r["key"] == key)
    assert first_rejection < later_success

    # Between the rejection and the successful delete the object must still be readable: that is the
    # half of the battery that proves the store did not honour the stale token.
    survived = [
        r
        for r in records
        if r["key"] == key
        and r["method"] in ("GET", "HEAD")
        and r["status"] == 200
        and first_rejection < r["seq"] < later_success
    ]
    assert survived, "the object was not observed alive between the stale and the matching DELETE"


@pytest.mark.parametrize("disk", sorted(CAS_DISKS))
def test_list_stays_unmarked_and_its_etag_never_becomes_a_cas_token(disk):
    """LIST keeps upstream ETag semantics on a CAS disk.

    Would fail if: LIST acquired the request mode — it would carry a translated header — or if a
    LIST-derived ETag were ever accepted as a generation, which the value-domain check catches.
    """
    records = _captured(CAS_DISKS[disk])
    lists = [r for r in records if r["method"] == "GET" and not r["key"]]
    assert lists, "no LIST reached the fake"
    for record in lists:
        assert not _is_translated(record), "a LIST carried a translated header: {}".format(
            record["query"]
        )

    etag_values = {_unquote(e) for e in _minted()["etags"]}
    for value in _generation_preconditions(records):
        assert value not in etag_values


def test_ordinary_gcp_oauth_traffic_keeps_upstream_semantics():
    """The upgrade regression this change exists to remove, checked on a non-CAS disk.

    The two absence assertions alone would be vacuous, and that is worth spelling out: a generation
    precondition is only ever emitted for a request that already carried `If-Match`/`If-None-Match`,
    and `x-goog-meta-*` only for one that carried `x-amz-meta-*`. An ordinary disk sends none of
    those, so marking every request on the client — the exact regression this plan removes — would
    leave those two assertions green. They are kept because they are cheap and true, not because they
    fence anything.

    The assertion that DOES fence it is the last one. Marking a request runs
    `prepareGcsRequestForOAuthAuthentication`, which deletes `x-amz-api-version`, so an ordinary
    request must still carry it. Would fail if: `Client::BuildHttpRequest` marked requests it should
    not — the header would vanish from this bucket.
    """
    records = _captured(PLAIN_BUCKET)
    assert records, "the ordinary disk sent no request, so this test would be vacuous"
    for record in records:
        assert "x-goog-if-generation-match" not in record["headers"], record["query"]
        assert not _has_goog_metadata(record), record["query"]

    observable = [r for r in records if r["method"] in _MARKING_OBSERVABLE_METHODS]
    assert observable, "no GET/HEAD/DELETE on the ordinary disk, so the check below would be vacuous"
    for record in observable:
        assert _looks_default_on_oauth(record), (
            "an ordinary {} on {} lost x-amz-api-version, so it was marked".format(
                record["method"], record["key"] or "(list)"
            )
        )


def test_marked_and_default_heads_coexist_on_one_oauth_client():
    """Per-request marking, observed on ONE client, for ONE method, in ONE bucket.

    The CAS `gcp_oauth` disk owns a single S3 client and issues HEADs of both kinds: CAS metadata
    reads are marked, while `probeSentinelRaw` deliberately goes through the ordinary throwing
    `getObjectMetadata` because it must tell no-such-key from no-such-bucket from a transient failure,
    and it discards the metadata anyway. Marking deletes `x-amz-api-version`, so the two kinds are
    distinguishable on the wire even though they are the same verb on the same key space.

    This is the assertion I earlier reported the fixture could not make. I was wrong for a specific
    reason worth keeping: marking adds no header, which is true, but it REMOVES one, and an absence is
    just as observable as a presence.

    Would fail if: every request were marked (the `Default` HEAD would lose the header) or none were
    (all the marked HEADs would keep it). Both directions fire, which is what makes it a partition
    rather than a one-sided check.

    Only the OAuth disk can support this. On `gcs_hmac`,
    `prepareGcsRequestForGoog4Authentication` runs for every request the client sends, so the header
    is absent regardless of mode and carries no information.
    """
    heads = [r for r in _captured(CAS_DISKS["cas_gcs_oauth"]) if r["method"] == "HEAD"]
    assert heads, "no HEAD reached the fake, so this test would be vacuous"

    default_heads = [r for r in heads if _looks_default_on_oauth(r)]
    marked_heads = [r for r in heads if not _looks_default_on_oauth(r)]

    assert marked_heads, "no HEAD was marked — CAS metadata reads lost their request mode"
    assert default_heads, (
        "every HEAD was marked — the sentinel probe's ordinary metadata read was marked too, "
        "which is the whole-client marking regression this plan removes"
    )


@pytest.mark.parametrize("disk", sorted(CAS_DISKS))
def test_translated_requests_are_confined_to_conditional_operations(disk):
    """Translated headers appear only where a precondition or custom metadata was actually sent.

    Weaker than the test above and deliberately kept for both disks, since it is the only isolation
    statement available on `gcs_hmac`: a request carrying no CAS precondition must carry no
    `x-goog-if-generation-match`, so a blanket translation would show up here as a generation
    precondition on a plain read.

    Would fail if: the dialect began emitting generation preconditions for requests that carried no
    ETag precondition — for instance by defaulting a missing precondition to `0`.
    """
    records = _captured(CAS_DISKS[disk])
    translated = [r for r in records if _is_translated(r)]
    assert translated, "nothing was translated at all, so this test would be vacuous"

    for record in records:
        if record["method"] == "GET" and not record["key"]:
            assert not _is_translated(record), "a LIST carried a translated header"

    # Every translated request is a mutation: a conditional PUT, a CAS-owned copy, or an exact DELETE.
    # A plain read must never acquire one.
    for record in translated:
        assert record["method"] in ("PUT", "DELETE", "POST"), (
            "a {} on {} carried a translated header but is not a mutation".format(
                record["method"], record["key"] or "(list)"
            )
        )


@pytest.mark.parametrize("disk", sorted(CAS_DISKS))
def test_the_fake_refused_nothing_it_had_to_serve(disk):
    """A `501 NotImplemented` from the fake means the mount needed an operation the fake refuses.

    That is a fixture gap, not a product bug, and it must not hide behind a passing suite. Would fail
    if: a CAS path started using multipart upload, versioning or another refused operation.
    """
    refused = [r for r in _captured(CAS_DISKS[disk]) if r["status"] == 501]
    assert not refused, "the fake refused operations it was asked for: {}".format(
        [(r["method"], r["key"], r["query"]) for r in refused]
    )


# ---------------------------------------------------------------------------------------------------
# Adversarial coverage. Everything below runs after the tests above on purpose: some of these restart
# the server or drive the fake into a mode no correct client provokes, and the assertions above read
# the whole capture log.
# ---------------------------------------------------------------------------------------------------

# A bucket no disk is configured against, so requests this file issues by hand are invisible to every
# per-bucket assertion above.
PROBE_BUCKET = "probebucket"


def test_the_fake_refuses_a_keyless_write_and_a_bucket_level_object_subresource():
    """Two request shapes that must not be served half-way.

    A keyless `PUT /bucket` is `CreateBucket`, which this fake does not model; served as a generic
    object write it would mint a phantom object at the empty key that then appears in every later
    listing of the bucket. `GET /bucket?tagging` is a bucket-level address for an object-level
    subresource; answered by the bare-listing shortcut it would return a full object listing to a
    caller that asked for a tag set.

    Negative control on the fixture, named as such: no production change flips this. It is asserted
    because both shapes would corrupt the capture log the tests above read, silently and in a way that
    reads as a ClickHouse bug.
    """
    assert _raw("PUT", "/{}".format(PROBE_BUCKET)) == 501
    assert _raw("GET", "/{}?tagging".format(PROBE_BUCKET)) == 501
    assert _raw("DELETE", "/{}".format(PROBE_BUCKET)) == 501

    listing = [
        r
        for r in _captured(PROBE_BUCKET)
        if r["method"] == "GET" and not r["key"] and r["status"] == 200
    ]
    assert not listing, "a bucket-level subresource was served as a listing"


def test_a_generation_in_the_etag_domain_is_caught_by_the_fake_but_only_in_its_strict_mode():
    """What each kind of real service would do with the request shape the design calls unsafe.

    The design refuses to assume whether GCS rejects, compares or ignores a numeric generation placed
    in an ETag-valued `If-Match`. This drives that shape by hand, under both of the fake's modes, and
    reads the two answers off the wire:

      - `reject` (the default): `400`, and the object survives. The mistake is loud.
      - `ignore`: `204`, and the object is GONE. The caller is told its exact delete succeeded when
        nothing was ever compared, which is data loss with no error anywhere.

    The conclusion is what makes this worth having, so state it rather than leave it implied: because
    a permissive service answers the unsafe shape with success, the fixture's safety CANNOT rest on
    the service's answer. `test_no_cas_request_sends_a_generation_as_an_etag` — which inspects the
    header CAS actually sent, whatever the service did with it — is the load-bearing fence, and this
    test is why.

    Negative control on the fixture: nothing in ClickHouse flips it. Production never sends this
    shape, which is exactly why it has to be driven by hand to be observed at all.
    """
    key = "cas-token-in-etag-domain"
    path = "/{}/{}".format(PROBE_BUCKET, key)

    try:
        for mode, expected_status, expected_after in (
            ("reject", 400, 200),
            ("ignore", 204, 404),
        ):
            assert _set_if_match_mode(mode)["if_match"] == mode
            assert _raw("PUT", path) == 200
            generation = _captured(PROBE_BUCKET)[-1]["response_generation"]
            assert generation and generation.isdigit(), generation

            assert (
                _raw("DELETE", path, [("If-Match", generation)]) == expected_status
            ), "mode {} answered the unsafe shape unexpectedly".format(mode)
            assert _raw("HEAD", path) == expected_after, (
                "mode {}: the object's survival does not match the delete's answer".format(mode)
            )
    finally:
        assert _set_if_match_mode("reject")["if_match"] == "reject"


def test_a_permissive_service_does_not_change_what_cas_puts_on_the_wire():
    """Remount the whole node against the permissive service and re-read every disk.

    The point is that correctness here is a property of the client, not of the store: under `ignore`
    the fake compares nothing when a generation arrives in the ETag domain, so a client that had lost
    its native mark would sail through the capability battery and mount successfully. The mount below
    still passes for the opposite reason — CAS never sends that shape at all — and the delta assertion
    is what says so.

    Would fail if: any CAS conditional operation lost its request mode. The precondition would move
    to `If-Match`, the permissive fake would swallow it, and `numeric_if_match` would be non-zero for
    a run in which every table still read back correctly. That is precisely the regression a strict
    fake would have masked as a loud mount failure and this one catches as a silent one.
    """
    node = cluster.instances["node"]
    tables = ["t_" + disk for disk in list(CAS_DISKS) + [PLAIN_DISK]]
    # Read the counts before the remount rather than comparing against NUM_ROWS: later tests in this
    # file insert more rows, and a constant here would make this test's correctness depend on where it
    # sits in the file.
    before_counts = {t: int(node.query("SELECT count() FROM {}".format(t))) for t in tables}
    before_sums = {t: int(node.query("SELECT sum(id) FROM {}".format(t))) for t in tables}
    assert all(count > 0 for count in before_counts.values()), before_counts

    before_numeric = _counters().get("numeric_if_match", 0)
    first_new_seq = _next_seq()
    try:
        assert _set_if_match_mode("ignore")["if_match"] == "ignore"
        node.restart_clickhouse()

        for table in tables:
            assert int(node.query("SELECT count() FROM {}".format(table))) == before_counts[table]
            assert int(node.query("SELECT sum(id) FROM {}".format(table))) == before_sums[table]
    finally:
        assert _set_if_match_mode("reject")["if_match"] == "reject"
        node.restart_clickhouse()

    # The remount must actually have reached the store, or every assertion below is vacuous. A fresh
    # mount runs the capability battery, so its generation preconditions are the strongest available
    # evidence that this is a new mount's traffic and not a replay of the log read above.
    remounted = _captured_since(first_new_seq)
    assert remounted, "the restart produced no request at all"
    for bucket in CAS_DISKS.values():
        fresh = _captured_since(first_new_seq, bucket)
        assert _generation_preconditions(fresh), (
            "no generation precondition after the remount of {}, so the capability battery did not "
            "run and this test proves nothing".format(bucket)
        )

    assert _counters().get("numeric_if_match", 0) == before_numeric, (
        "a request put a numeric value in the ETag domain while the fake was permissive enough to "
        "accept it"
    )


def test_a_token_producing_write_never_fragments_into_a_multipart_upload():
    """No multipart operation in the traffic the fake has seen UP TO THIS POINT in the module.

    The counters are global and cumulative and this test is not last in the file, so its reach is a
    PREFIX of the run — it covers the mount, the initial fills and every test above it, and nothing
    below. `test_no_multipart_operation_was_issued_anywhere_in_the_whole_run` at the end of the file is
    what makes the run-wide claim; this one localises a failure to the first half.

    GCS enforces no precondition on `CompleteMultipartUpload`, so a token-producing write that
    fragmented would lose its write-once guarantee at the moment of completion.
    `tokenProducingWriteSettings` therefore forces a single part for the Generation dialect.

    Would fail if: `s3_force_single_part_upload` stopped being set for the Generation dialect and a
    blob crossed the ordinary multipart threshold.

    What this does NOT cover, and cannot from here: the ABOVE-THE-CAP arm, where a token-producing
    body larger than `token_producing_single_put_cap` must be refused before any request is issued.
    That cap is a 1 GiB constructor default with no configuration key, so an integration test cannot
    reach it. `CopyObjectConditionalAboveSinglePutCapThrowsNotImplementedBeforeAnyRequest` covers it
    at unit level.

    It also overlaps `test_the_fake_refused_nothing_it_had_to_serve`, which would catch a multipart
    attempt as a `501` — but only in the two CAS buckets, and only once it happened. The counters
    below distinguish "never attempted" from "attempted and refused".
    """
    counters = _counters()
    assert counters.get("method_PUT", 0) > 0, (
        "no PUT was ever sent, so the absence of multipart operations proves nothing"
    )
    for name in ("CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload"):
        assert counters.get(name, 0) == 0, "{} was issued {} time(s)".format(
            name, counters[name]
        )


def test_interleaved_ordinary_and_cas_operations_do_not_leak_mode_or_build_a_client():
    """Two statements over one interleaved workload on the OAuth clients.

    Mode isolation: the ordinary disk's requests must still carry `x-amz-api-version` (marking deletes
    it) and the CAS disk's traffic must still be marked, in a slice of the log where the two disks'
    traffic is interleaved rather than separated by phase. The contribution here is the INTERLEAVING;
    that a single OAuth client carries both marked and unmarked requests is established separately by
    `test_marked_and_default_heads_coexist_on_one_oauth_client`, which asserts both halves non-empty in
    one bucket. This test asserts only the marked half, deliberately -- duplicating the partition would
    add a second place to keep in step and no new fencing power. Would fail if: the mode became a
    property of the client rather than of the request.

    Client count: the metadata server hands out a token when a client's cache is first populated, and
    answers a 24-hour expiry, so within this slice a new token fetch means a new client with a new
    token cache. Zero new fetches says the request mode built neither. Would fail if: selecting the
    request mode constructed a third client — the base and single-attempt clients already exist by
    this point, having been built during the mount and the first conditional write.

    The `> 0` preconditions matter three times here. Without the store-traffic check the mode
    assertions would hold over an empty slice; without the per-method check they would hold over a
    slice containing only writes; and without the lifetime-total token check the `== 0` would hold on
    a fixture whose metadata server was never reached at all.

    The read has to be one that MUST reach object storage, and the first version of this test got that
    wrong: it used `SELECT count()`, which is answered from part metadata and never fetched a column,
    so the ordinary disk's slice held nothing but PUTs and the per-method precondition below caught it.
    A column read of the rows just inserted, with the mark and uncompressed caches dropped first, is
    what actually issues a GET.
    """
    node = cluster.instances["node"]
    assert _token_fetches() > 0, (
        "the metadata server was never asked for a token, so counting new fetches proves nothing"
    )

    first_new_seq = _next_seq()
    _reset_token_fetches()

    for round_index in range(3):
        base = 10000 + round_index * 100
        for disk in (PLAIN_DISK, "cas_gcs_oauth"):
            table = "t_" + disk
            node.query(
                "INSERT INTO {} SELECT number, toString(number) FROM numbers({}, 10)".format(
                    table, base
                )
            )
            # Drop the caches that would otherwise answer the read from memory, then read a COLUMN of
            # the rows just written rather than a count.
            node.query("SYSTEM DROP MARK CACHE")
            node.query("SYSTEM DROP UNCOMPRESSED CACHE")
            assert (
                int(node.query("SELECT sum(id) FROM {} WHERE id >= {}".format(table, base)))
                >= base
            )

    cas_bucket = CAS_DISKS["cas_gcs_oauth"]
    plain = _captured_since(first_new_seq, PLAIN_BUCKET)
    cas = _captured_since(first_new_seq, cas_bucket)
    assert plain, "the ordinary disk sent nothing in this slice"
    assert cas, "the CAS disk sent nothing in this slice"

    # Named explicitly rather than folded into the `observable_plain` check, so that a workload which
    # stops reaching the store says WHICH method vanished instead of just going quiet. Only GET is
    # required: marking is a per-request property, so one observable request is enough to fence it, and
    # forcing a DELETE would mean waiting on part-removal timing.
    assert [r for r in plain if r["method"] == "GET" and r["key"]], (
        "the ordinary disk issued no object GET in this slice, so the read never reached the store"
    )

    observable_plain = [r for r in plain if r["method"] in _MARKING_OBSERVABLE_METHODS]
    assert observable_plain, "no GET/HEAD/DELETE on the ordinary disk in this slice"
    for record in observable_plain:
        assert _looks_default_on_oauth(record), (
            "an interleaved ordinary {} on {} was marked".format(
                record["method"], record["key"] or "(list)"
            )
        )

    observable_cas = [r for r in cas if r["method"] in _MARKING_OBSERVABLE_METHODS]
    assert [r for r in observable_cas if not _looks_default_on_oauth(r)], (
        "no CAS request in this slice was marked"
    )

    new_fetches = _token_fetches()
    assert new_fetches == 0, (
        "an interleaved workload fetched {} new metadata token(s), so it built a client with a new "
        "token cache".format(new_fetches)
    )


def test_a_write_whose_response_carries_no_generation_is_refused():
    """The one input that can reach the "no valid generation" refusal.

    A real GCS always answers a successful object write with `x-goog-generation`, and the response
    adapter turns that into the SDK's `ETag`. When it is absent the SDK sees the store's real ETag
    instead, which is not a generation, so `tokenFromWriteResult` must refuse to attribute the write to
    an incarnation rather than patching the missing token over with a fresh HEAD — a HEAD returns
    whatever incarnation happens to be current, which on a lost race is somebody else's.

    The error text is the whole discriminator, and it is tight: had the code HEADed and adopted the
    current incarnation instead of refusing, the INSERT would have SUCCEEDED. It failed, naming the
    missing generation. So a regression that replaced the strict branch with a HEAD-and-adopt fallback
    turns the error assertion red on its own.

    Do NOT add an assertion here about which requests follow that write. Two forms were tried and both
    had to be removed, for a reason no assertion on this seam can escape: what follows depends on WHICH
    controlled lane the refused write happened to be on, and which object gets refused first varies
    between runs. `putIfAbsentControlled` treats any raising attempt as unresolved and then runs
    `resolveByExactGet`, so a HEAD and a GET of the key follow; `conditionalCreateControlled` filters
    the attempt through `isDeterministicLocalFailure`, which lists `CORRUPTED_DATA`, and propagates
    without resolving, so nothing follows at all. A manifest write takes the first lane and a blob write
    the second, and nothing on the wire distinguishes a manifest put from a blob create — so the test
    cannot know whether a resolve is expected, and `omit_generation` is global, so which object draws the
    short straw varies between runs.

    What fences the behaviour is the error text above, and nothing else here needs to.

    Would fail if: the strict Generation branch in `tokenFromWriteResult` were replaced by, or fell
    back to, the ETag dialect's HEAD path.

    The mode is global while it is on, so a background CAS operation on the other disk can fail during
    the window too. That is logged, not fatal, and the restored-mode INSERT at the end is what says
    the disk is healthy again.
    """
    node = cluster.instances["node"]
    table = "t_cas_gcs_oauth"
    cas_bucket = CAS_DISKS["cas_gcs_oauth"]

    first_new_seq = _next_seq()
    try:
        assert _set_omit_generation(True)["omit_generation"] is True
        error = node.query_and_get_error(
            "INSERT INTO {} SELECT number, toString(number) FROM numbers(20000, 50)".format(table)
        )
    finally:
        assert _set_omit_generation(False)["omit_generation"] is False

    assert "carried no valid generation" in error, error

    # Positive proof that the fake actually produced the condition under test: a successful object
    # write really did answer without a generation. Without this the error assertion above could be
    # satisfied by an INSERT that failed for some entirely unrelated reason, and a mode switch that
    # silently stopped working would look like a pass.
    ungenerated = [
        r
        for r in _captured_since(first_new_seq, cas_bucket)
        if r["method"] == "PUT" and r["status"] == 200 and r["response_generation"] is None
    ]
    assert ungenerated, "the mode was on but no successful PUT answered without a generation"

    # Restoring the mode must restore the disk, or the failure above was something other than the
    # missing generation.
    node.query("INSERT INTO {} SELECT number, toString(number) FROM numbers(30000, 50)".format(table))
    assert int(node.query("SELECT count() FROM {} WHERE id >= 30000".format(table))) == 50


# ---------------------------------------------------------------------------------------------------
def test_a_reload_that_would_flip_the_token_dialect_is_refused():
    """A live CAS mount must keep the incarnation-token dialect it was opened with.

    The pool derives persistent state from that dialect -- how a token value is normalised, whether a
    listing may supply one at all, and which preconditions the mount had to satisfy -- so a reload that
    swapped the client for one minting the other kind would leave persisted tokens uncomparable.

    WHAT THIS TEST DOES NOT PROVE, stated because the obvious reading is wrong. It flips the DISK-LEVEL
    `http_client`, which a guard reading only the disk section would also have refused. So it does not
    discriminate where the check lives; it only shows that a flip is refused and that the old client
    survives. The placement is what actually matters -- the effective value is merged from the storage's
    current settings, any endpoint-level block and the disk section, so a disk-section-only check misses
    a flip arriving from an endpoint block and falsely refuses a no-op reload whenever the effective
    value comes from elsewhere -- and that property is covered by reading the code, not by this test.
    Writing the discriminating version needs a CAS mount pinned to ETag, which this fixture cannot host:
    the fake mints numeric ETags, so an ETag-dialect mount sends a numeric `If-Match` and the fake's own
    domain check rejects it as a generation reaching the ETag domain. Teaching it a second ETag shape is
    the prerequisite, and is deliberately not done here.

    Asserting the refusal is not enough on its own, because "the reload was refused" and "the reload was
    refused AND the old client survived" are different claims and only the second is the guarantee. So
    the test also shows the mount still speaks generation afterwards: a fresh conditional write still
    carries a numeric precondition, which only a generation-dialect client sends.

    Would fail if: the pin were not installed at startup, or were checked after the client had already
    been replaced. It would NOT fail if the pin read a single config section, which is the gap above.
    """
    node = cluster.instances["node"]
    table = "t_cas_gcs_oauth"
    cas_bucket = CAS_DISKS["cas_gcs_oauth"]


    try:
        # An explicit non-GCS value, not a removed key. Settings merge through `updateIfChanged`, which
        # applies only values the incoming config actually SET, so deleting `http_client` leaves the old
        # one in force and flips nothing -- the first version of this test deleted it and the guard
        # correctly stayed silent. No validation rejects an unrecognised value; it simply selects the
        # ordinary client, which is an ETag store.
        node.replace_in_config(
            CONFIG_IN_CONTAINER,
            "<http_client>gcp_oauth</http_client>",
            "<http_client>none</http_client>",
        )
        try:
            reload_error = node.query_and_get_error_with_retry(
                "SYSTEM RELOAD CONFIG", retry_count=1, sleep_time=0
            )
        except Exception:
            reload_error = ""

        # Whether the refusal reaches the client or only the log depends on how config reload reports a
        # failing disk, so accept either -- but require one of them, and require the specific reason rather
        # than any failure.
        logged = node.grep_in_log("cannot change its conditional-operation dialect on reload")
        assert "conditional-operation dialect" in reload_error or logged, (
            "the reload was neither refused to the client nor recorded as refused in the log; "
            "error was {!r}".format(reload_error)
        )

        # The guarantee: the old client survived, so this mount still speaks the generation dialect. The
        # evidence is a generation PRECONDITION on the wire, which only a generation-dialect client sends.
        # Not the `numeric_if_match` counter -- that one counts `If-Match` (exact-token) requests, and an
        # INSERT sends `x-goog-if-generation-match` for create-if-absent instead, so the counter would have
        # stayed flat here for a reason that has nothing to do with the dialect.
        after_reload_seq = _next_seq()
        node.query(
            "INSERT INTO {} SELECT number, toString(number) FROM numbers(30000, 20)".format(table)
        )
        post_reload = _captured_since(after_reload_seq, cas_bucket)
        assert post_reload, "the INSERT after the refused reload reached the store not at all"
        conditional = [r for r in post_reload if "x-goog-if-generation-match" in r["headers"]]
        assert conditional, (
            "no generation precondition was sent after the refused reload, so the mount is no longer "
            "speaking the generation dialect it was opened with"
        )
    finally:
        # Always restore, even on a failed assertion: leaving the disk configured for the other
        # dialect would break every test that runs after this one.
        node.replace_in_config(
            CONFIG_IN_CONTAINER,
            "<http_client>none</http_client>",
            "<http_client>gcp_oauth</http_client>",
        )
        node.query("SYSTEM RELOAD CONFIG")
        node.query(
            "INSERT INTO {} SELECT number, toString(number) FROM numbers(31000, 20)".format(table)
        )


# MUST STAY LAST IN THIS FILE. The fake's counters are global and cumulative and nothing in this module
# resets them, so a counter assertion covers exactly the traffic that precedes it. Placed anywhere but
# last, the test below silently becomes a prefix check and stops seeing the traffic of whatever now
# follows it — which is how it read before, and the reason the run-wide claim was not being made.
# Add new tests ABOVE this line.
# ---------------------------------------------------------------------------------------------------


def test_no_multipart_operation_was_issued_anywhere_in_the_whole_run():
    """The run-wide form of the single-part invariant, over every request the fake ever saw.

    On a generation-token store a token-producing write must stay one PUT: GCS enforces no
    precondition on `CompleteMultipartUpload`, so a fragmented write would lose its write-once
    guarantee at the moment of completion, which is why the single-part cap exists at all. The claim
    worth making is therefore the unqualified one — nothing in this module, at any point, fragmented.

    Would fail if: `s3_force_single_part_upload` stopped being set for the Generation dialect, or any
    later-added scenario drove a write over the ordinary multipart threshold. Unlike the
    partway-through check above, a regression introduced by the last test in the file is caught here.

    `method_PUT > 0` keeps it from passing vacuously: on a module where nothing ever wrote, three
    absent counters would prove nothing at all.
    """
    counters = _counters()
    assert counters.get("method_PUT", 0) > 0, (
        "no PUT was ever sent in the entire run, so the absence of multipart operations proves nothing"
    )
    for name in ("CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload"):
        assert counters.get(name, 0) == 0, "{} was issued {} time(s) during the run".format(
            name, counters[name]
        )
