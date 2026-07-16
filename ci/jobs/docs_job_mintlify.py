import argparse

from ci.jobs.scripts.docs.check_readonly_copies import check_readonly_copies
from ci.jobs.scripts.docs.mintlify_docs_check import (
    DEFAULT_CHECKS,
    LOCALE_CHECKS,
    QUICKSTARTS_CHECK,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Utils

# The translated trees that docs.json ships (via `languages` + `$ref`s): both the
# top-level locale pages and the localized snippet components they render. The
# locale checks run only when a PR touches one of these folders -- or the
# checkers themselves -- so ordinary English-only edits don't pay for them.
LOCALE_DIRS = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"]
LOCALE_CHECK_TRIGGERS = tuple(f"docs/{d}/" for d in LOCALE_DIRS) + (
    "ci/jobs/scripts/docs/lychee_check.py",
    "ci/jobs/scripts/docs/locale_components_check.py",
    "docs/lychee.toml",
    # Shared inputs the locale resolvers depend on, so a PR that touches them can
    # break localized navigation and must re-run the locale checks even when no
    # docs/<locale>/ page changed:
    #  - all of snippets/ (locale pages import/link both localized snippets and
    #    shared English snippets, e.g. /snippets/delete);
    #  - redirect sources, which count as valid locale link targets (e.g.
    #    /<locale>/integrations resolves only via a redirect).
    "docs/snippets/",
    "docs/_site/redirects.json",
)

# The quickstarts check (frontmatter tags + generated explorer data freshness)
# runs only when a PR touches a quickstarts tree, the generated data modules,
# the generator, or the checker itself.
QUICKSTARTS_CHECK_TRIGGERS = (
    ("docs/get-started/quickstarts/",)
    + tuple(f"docs/{d}/get-started/quickstarts/" for d in LOCALE_DIRS)
    + ("docs/snippets/components/QuickStartsGrid/",)
    + tuple(f"docs/snippets/{d}/components/QuickStartsGrid/" for d in LOCALE_DIRS)
    + (
        "docs/_site/scripts/update_quickstarts.py",
        "ci/jobs/scripts/docs/quickstarts_check.py",
    )
)


def parse_args():
    parser = argparse.ArgumentParser(description="Mintlify Docs Check Job")
    parser.add_argument("--test", help="Sub check name", default="")
    return parser.parse_args()


def _locale_check_should_run():
    # Run the locale link-check only when the PR changes a locale tree or the
    # checker. If the changed-file list is unavailable, run it (fail open toward
    # coverage: a missed run would let broken locale links merge).
    changed_files = Info().get_changed_files()
    if changed_files is None:
        return True
    return any(f.startswith(LOCALE_CHECK_TRIGGERS) for f in changed_files)


def _quickstarts_check_should_run():
    # Same gating idea as the locale checks: run only when the PR touches the
    # quickstarts content or its tooling; fail open toward coverage when the
    # changed-file list is unavailable.
    changed_files = Info().get_changed_files()
    if changed_files is None:
        return True
    return any(f.startswith(QUICKSTARTS_CHECK_TRIGGERS) for f in changed_files)


def _readonly_copies_guard():
    # One-way sync: fail edits to docs folders whose canonical source lives in
    # another repo (declared in ci/jobs/scripts/docs/readonly_copies.json). This
    # is aggregator-only -- the consuming repos are the source of truth, so this
    # is deliberately not part of the shared DEFAULT_CHECKS.
    changed_files = Info().get_changed_files()
    if changed_files is None:
        # Fail close: without the changed-file list we cannot prove the PR does
        # not touch read-only copies, so do not report success.
        print("Error: the changed-file list is unavailable, cannot run the check.")
        return False
    return check_readonly_copies(changed_files)


if __name__ == "__main__":

    args = parse_args()
    testpattern = args.test

    docs_dir = f"{Utils.cwd()}/docs"

    def selected(name):
        # Case-insensitive substring match; an empty pattern runs every check.
        return testpattern.lower() in name.lower()

    # The mint check definitions are shared with the standalone driver
    # (ci/jobs/scripts/docs/mintlify_docs_check.py). This job already runs inside
    # the docs-builder image with the docs present natively, so it runs them
    # directly; add new checks to DEFAULT_CHECKS, not here. --test selects a
    # subset by sub-check name.
    results = [
        Result.from_commands_run(name=name, command=command, workdir=docs_dir)
        for name, command in DEFAULT_CHECKS
        if selected(name)
    ]

    # Locale checks: blocking, but only when the PR touches a locale tree,
    # a localized snippet, or a checker. This is how a GT translation PR that
    # reintroduces broken/unlocalized locale links gets caught before merge.
    if _locale_check_should_run():
        for locale_name, locale_command in LOCALE_CHECKS:
            if selected(locale_name):
                results.append(
                    Result.from_commands_run(
                        name=locale_name, command=locale_command, workdir=docs_dir
                    )
                )

    # Quickstarts check: blocking, but only when the PR touches the quickstarts
    # trees or their tooling. Validates frontmatter tags and fails when the
    # generated explorer data was not regenerated and committed.
    if _quickstarts_check_should_run():
        qs_name, qs_command = QUICKSTARTS_CHECK
        if selected(qs_name):
            results.append(
                Result.from_commands_run(
                    name=qs_name, command=qs_command, workdir=docs_dir
                )
            )

    # The read-only guard runs from the repo root (not the docs root), so keep
    # it out of the docs_dir loop above.
    if selected("No direct edits to read-only copied docs"):
        results.append(
            Result.from_commands_run(
                name="No direct edits to read-only copied docs",
                command=_readonly_copies_guard,
            )
        )

    Result.create_from(results=results).complete_job()