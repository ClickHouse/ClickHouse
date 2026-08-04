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
            ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"],
            [],
        ),
        (["src/Core/TypeId.h"], []),
        ([], []),
    ],
)
def test_get_docs_teams_to_request(changed_files, expected_teams):
    assert team_notifications.get_docs_teams_to_request(changed_files) == expected_teams


def test_check_requests_docs_teams(monkeypatch):
    class FakeInfo:
        def get_kv_data(self, key):
            assert key == "changed_files"
            return [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/language-clients/python/index.mdx",
            ]

    synced = {}

    def fake_sync(desired_teams, managed_teams):
        synced["desired"] = desired_teams
        synced["managed"] = managed_teams

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH, "sync_team_review_requests", staticmethod(fake_sync)
    )

    assert team_notifications.check()
    assert synced == {
        "desired": ["clickpipes", "integrations-ecosystem", "docs"],
        "managed": ("docs", "clickpipes", "integrations-ecosystem"),
    }


def test_check_removes_review_requests_when_docs_only_pr_becomes_mixed(monkeypatch):
    changed_files = iter(
        [
            ["docs/integrations/clickpipes/home.mdx"],
            ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"],
        ]
    )

    class FakeInfo:
        def get_kv_data(self, key):
            assert key == "changed_files"
            return next(changed_files)

    synced = []

    def fake_sync(desired_teams, managed_teams):
        synced.append((desired_teams, managed_teams))

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH,
        "sync_team_review_requests",
        staticmethod(fake_sync),
    )

    assert team_notifications.check()
    assert team_notifications.check()
    assert synced == [
        (
            ["clickpipes", "docs"],
            ("docs", "clickpipes", "integrations-ecosystem"),
        ),
        ([], ("docs", "clickpipes", "integrations-ecosystem")),
    ]


def test_check_preserves_existing_type_id_notification(monkeypatch):
    class FakeInfo:
        def get_kv_data(self, key):
            assert key == "changed_files"
            return ["src/Core/TypeId.h"]

    posted = {}

    def fake_post(comment_tags_and_bodies):
        posted.update(comment_tags_and_bodies)

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH,
        "sync_team_review_requests",
        staticmethod(lambda desired_teams, managed_teams: None),
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
        def get_kv_data(self, key):
            assert key == "changed_files"
            return None

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)

    with pytest.raises(AssertionError, match="changed_files is not populated"):
        team_notifications.check()
