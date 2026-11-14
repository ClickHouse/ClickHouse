import pytest

from ci.jobs.scripts import team_review_requests


def make_event(*, internal, labels=None):
    repository = "ClickHouse/ClickHouse"
    return {
        "repository": {"full_name": repository},
        "pull_request": {
            "number": 42,
            "base": {"repo": {"full_name": repository}},
            "head": {
                "repo": {
                    "full_name": repository if internal else "contributor/ClickHouse"
                }
            },
            "labels": [{"name": label} for label in labels or []],
        },
    }


@pytest.mark.parametrize(
    ("changed_files", "expected_teams"),
    [
        (["docs/reference/functions/array-functions.mdx"], ["docs"]),
        (
            ["docs/integrations/clickpipes/kafka/index.mdx"],
            ["clickpipes", "docs"],
        ),
        (
            ["docs/integrations/language-clients/python/index.mdx"],
            ["integrations-ecosystem", "docs"],
        ),
        (
            ["docs/integrations/connectors/data-ingestion/index.mdx"],
            ["integrations-ecosystem", "docs"],
        ),
        (
            [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/connectors/navigation.json",
            ],
            ["clickpipes", "integrations-ecosystem", "docs"],
        ),
        (
            ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"],
            [],
        ),
        (
            [
                "ci/jobs/scripts/team_review_requests.py",
                "docs/integrations/clickpipes/home.mdx",
            ],
            ["clickpipes", "docs"],
        ),
        (["src/Core/TypeId.h"], []),
        ([], []),
    ],
)
def test_get_docs_teams_to_request(changed_files, expected_teams):
    assert (
        team_review_requests.get_docs_teams_to_request(changed_files)
        == expected_teams
    )


def test_check_requests_teams_for_internal_pr(monkeypatch):
    requested = []
    monkeypatch.setattr(
        team_review_requests,
        "get_changed_files",
        lambda repository, pr: ["docs/integrations/clickpipes/home.mdx"],
    )
    monkeypatch.setattr(
        team_review_requests.GH,
        "request_team_reviews",
        staticmethod(
            lambda teams, pr, repo: requested.append((teams, pr, repo)) or True
        ),
    )

    assert team_review_requests.check(make_event(internal=True))
    assert requested == [
        (["clickpipes", "docs"], 42, "ClickHouse/ClickHouse")
    ]


def test_check_requests_teams_for_approved_fork_pr(monkeypatch):
    requested = []
    monkeypatch.setattr(
        team_review_requests,
        "get_changed_files",
        lambda repository, pr: ["docs/integrations/connectors/index.mdx"],
    )
    monkeypatch.setattr(
        team_review_requests.GH,
        "request_team_reviews",
        staticmethod(
            lambda teams, pr, repo: requested.append((teams, pr, repo)) or True
        ),
    )

    assert team_review_requests.check(
        make_event(internal=False, labels=["can be tested"])
    )
    assert requested == [
        (["integrations-ecosystem", "docs"], 42, "ClickHouse/ClickHouse")
    ]


def test_check_skips_unapproved_fork_pr(monkeypatch):
    monkeypatch.setattr(
        team_review_requests,
        "get_changed_files",
        lambda *_args: pytest.fail("unexpected changed-files request"),
    )
    monkeypatch.setattr(
        team_review_requests.GH,
        "request_team_reviews",
        staticmethod(
            lambda *_args, **_kwargs: pytest.fail("unexpected review request")
        ),
    )

    assert team_review_requests.check(make_event(internal=False))


def test_get_changed_files_uses_paginated_pull_request_api(monkeypatch):
    commands = []

    def fake_get(command, strict=False):
        commands.append((command, strict))
        return "docs/new.mdx\ndocs/old.mdx\ndocs/new.mdx"

    monkeypatch.setattr(
        team_review_requests.GH,
        "get_output_with_retries",
        staticmethod(fake_get),
    )

    assert team_review_requests.get_changed_files("ClickHouse/ClickHouse", 42) == [
        "docs/new.mdx",
        "docs/old.mdx",
    ]
    assert len(commands) == 1
    assert "pulls/42/files" in commands[0][0]
    assert "--paginate" in commands[0][0]
    assert commands[0][1]


@pytest.mark.parametrize(
    ("repository", "pull_request_number"),
    [
        ("ClickHouse/ClickHouse;echo bad", 42),
        ("ClickHouse/ClickHouse", 0),
        ("ClickHouse/ClickHouse", "42"),
    ],
)
def test_get_changed_files_rejects_invalid_target(repository, pull_request_number):
    with pytest.raises(ValueError):
        team_review_requests.get_changed_files(repository, pull_request_number)
