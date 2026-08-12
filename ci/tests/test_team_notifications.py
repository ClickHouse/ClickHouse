import os
import subprocess
from types import SimpleNamespace

import pytest

from ci.jobs.scripts.workflow_hooks import team_notifications


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
                "docs/integrations/language-clients/python/index.mdx",
                "docs/integrations/connectors/data-ingestion/index.mdx",
            ],
            ["integrations-ecosystem", "docs"],
        ),
        (
            [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/connectors/navigation.json",
            ],
            [
                "clickpipes",
                "integrations-ecosystem",
                "docs",
            ],
        ),
        (
            [
                "docs/integrations/clickpipes/home.mdx",
                "docs/reference/functions/array-functions.mdx",
            ],
            ["clickpipes", "docs"],
        ),
        (
            [
                "docs/pt-BR/integrations/clickpipes/home.mdx",
                "docs/ja/integrations/connectors/navigation.json",
            ],
            ["docs"],
        ),
        (
            ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"],
            [],
        ),
        (
            ["docs/reference/functions/array-functions.mdx", "src/Core/Block.cpp"],
            [],
        ),
        (
            [
                "ci/jobs/scripts/workflow_hooks/team_notifications.py",
                "docs/integrations/clickpipes/home.mdx",
            ],
            ["clickpipes", "docs"],
        ),
        (["src/Core/TypeId.h"], []),
        ([], []),
    ],
)
def test_get_docs_teams_to_request(changed_files, expected_teams):
    assert team_notifications.get_docs_teams_to_request(changed_files) == expected_teams


def test_request_docs_team_reviews_uses_isolated_robot_session(monkeypatch):
    auth_calls = []
    requests = []

    monkeypatch.setenv("GH_CONFIG_DIR", "original-config")
    monkeypatch.setenv("GH_TOKEN", "app-token")
    monkeypatch.setenv("GITHUB_TOKEN", "workflow-token")
    monkeypatch.setattr(
        team_notifications,
        "TEAM_REVIEW_TOKENS",
        (SimpleNamespace(name="robot", get_value=lambda: "robot-token"),),
    )

    def fake_auth(command, input, text, check, env):
        auth_calls.append((command, input, text, check, env))

    def fake_request(team_slugs):
        requests.append(
            (
                team_slugs,
                os.environ["GH_CONFIG_DIR"],
                os.environ.get("GH_TOKEN"),
                os.environ.get("GITHUB_TOKEN"),
            )
        )
        return True

    monkeypatch.setattr(team_notifications.subprocess, "run", fake_auth)
    monkeypatch.setattr(
        team_notifications.GH,
        "request_team_reviews",
        staticmethod(fake_request),
    )

    assert team_notifications.request_docs_team_reviews(["clickpipes", "docs"])
    assert len(auth_calls) == 1
    command, token, text, check, auth_env = auth_calls[0]
    assert command == ["gh", "auth", "login", "--with-token"]
    assert token == "robot-token"
    assert text and check
    assert auth_env["GH_CONFIG_DIR"] != "original-config"
    assert "GH_TOKEN" not in auth_env
    assert "GITHUB_TOKEN" not in auth_env
    assert requests == [
        (["clickpipes", "docs"], auth_env["GH_CONFIG_DIR"], None, None)
    ]
    assert os.environ["GH_CONFIG_DIR"] == "original-config"
    assert os.environ["GH_TOKEN"] == "app-token"
    assert os.environ["GITHUB_TOKEN"] == "workflow-token"


def test_request_docs_team_reviews_tries_next_robot(monkeypatch):
    tokens = []

    monkeypatch.setattr(
        team_notifications,
        "TEAM_REVIEW_TOKENS",
        (
            SimpleNamespace(name="robot-1", get_value=lambda: "bad-token"),
            SimpleNamespace(name="robot-2", get_value=lambda: "good-token"),
        ),
    )

    def fake_auth(command, input, text, check, env):
        tokens.append(input)
        if input == "bad-token":
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(team_notifications.subprocess, "run", fake_auth)
    monkeypatch.setattr(
        team_notifications.GH,
        "request_team_reviews",
        staticmethod(lambda team_slugs: team_slugs == ["docs"]),
    )

    assert team_notifications.request_docs_team_reviews(["docs"])
    assert tokens == ["bad-token", "good-token"]


def test_check_requests_docs_teams(monkeypatch):
    class FakeInfo:
        event_action = "opened"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/language-clients/python/index.mdx",
            ]

    requested = []

    def fake_request(team_slugs):
        requested.extend(team_slugs)

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications, "request_docs_team_reviews", fake_request
    )

    assert team_notifications.check()
    assert requested == ["clickpipes", "integrations-ecosystem", "docs"]


def test_check_does_not_request_reviews_without_docs_teams(monkeypatch):
    class FakeInfo:
        event_action = "opened"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return ["src/Core/Block.cpp"]

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications,
        "request_docs_team_reviews",
        staticmethod(lambda teams: not teams),
    )

    assert team_notifications.check()


def test_check_requests_docs_teams_after_update(monkeypatch):
    class FakeInfo:
        event_action = "synchronize"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return ["docs/integrations/clickpipes/home.mdx"]

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    requested = []

    monkeypatch.setattr(
        team_notifications,
        "request_docs_team_reviews",
        lambda team_slugs: requested.extend(team_slugs),
    )

    assert team_notifications.check()
    assert requested == ["clickpipes", "docs"]


def test_check_preserves_existing_type_id_notification(monkeypatch):
    class FakeInfo:
        event_action = "synchronize"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return ["src/Core/TypeId.h"]

    posted = {}

    def fake_post(comment_tags_and_bodies):
        posted.update(comment_tags_and_bodies)

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications,
        "request_docs_team_reviews",
        lambda teams: not teams,
    )
    monkeypatch.setattr(
        team_notifications.GH, "post_updateable_comment", staticmethod(fake_post)
    )

    assert team_notifications.check()
    assert posted == {
        "team_notification": "@ClickHouse/integrations team,  please, take a look"
    }


def test_check_fails_when_changed_files_are_unavailable(monkeypatch):
    class FakeInfo:
        event_action = "opened"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return None

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)

    with pytest.raises(AssertionError, match="changed_files is not populated"):
        team_notifications.check()
