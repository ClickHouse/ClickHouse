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
                "docs/pt-BR/integrations/clickpipes/home.mdx",
                "docs/ja/integrations/connectors/navigation.json",
            ],
            ["docs"],
        ),
        (
            ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"],
            [],
        ),
        (["src/Core/TypeId.h"], []),
        ([], []),
    ],
)
def test_get_docs_teams_to_request(changed_files, expected_teams):
    assert team_notifications.get_docs_teams_to_request(changed_files) == expected_teams


def test_check_skips_docs_teams_without_repository_access(monkeypatch):
    class FakeInfo:
        event_action = "opened"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/language-clients/python/index.mdx",
            ]

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH,
        "request_team_reviews",
        staticmethod(lambda *_args: pytest.fail("unexpected review request")),
    )

    assert team_notifications.check()


def test_check_requests_docs_teams_when_enabled(monkeypatch):
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
        team_notifications, "ENABLE_DOCS_TEAM_REVIEW_REQUESTS", True
    )
    monkeypatch.setattr(
        team_notifications.GH, "request_team_reviews", staticmethod(fake_request)
    )

    assert team_notifications.check()
    assert requested == ["clickpipes", "integrations-ecosystem", "docs"]


def test_check_does_not_manage_docs_reviews_after_open(monkeypatch):
    class FakeInfo:
        event_action = "synchronize"

        def get_kv_data(self, key):
            assert key == "changed_files"
            return ["docs/integrations/clickpipes/home.mdx"]

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH,
        "request_team_reviews",
        staticmethod(lambda *_args: pytest.fail("unexpected review request")),
    )

    assert team_notifications.check()


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
        team_notifications.GH,
        "request_team_reviews",
        staticmethod(lambda *_args: pytest.fail("unexpected review request")),
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
