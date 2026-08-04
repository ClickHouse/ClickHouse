import pytest

from ci.jobs.scripts.workflow_hooks import team_notifications


@pytest.mark.parametrize(
    ("changed_files", "expected_teams"),
    [
        (["docs/reference/functions/array-functions.mdx"], ["@ClickHouse/docs"]),
        (
            ["docs/integrations/clickpipes/kafka/index.mdx"],
            ["@ClickHouse/clickpipes", "@ClickHouse/docs"],
        ),
        (
            [
                "docs/integrations/language-clients/python/index.mdx",
                "docs/integrations/connectors/data-ingestion/index.mdx",
            ],
            ["@ClickHouse/integrations-ecosystem", "@ClickHouse/docs"],
        ),
        (
            [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/connectors/navigation.json",
            ],
            [
                "@ClickHouse/clickpipes",
                "@ClickHouse/integrations-ecosystem",
                "@ClickHouse/docs",
            ],
        ),
        (
            ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"],
            [],
        ),
        (["src/Core/TypeId.h"], ["@ClickHouse/integrations"]),
        ([], []),
    ],
)
def test_get_teams_to_notify(changed_files, expected_teams):
    assert team_notifications.get_teams_to_notify(changed_files) == expected_teams


def test_check_posts_one_updateable_comment(monkeypatch):
    class FakeInfo:
        def get_kv_data(self, key):
            assert key == "changed_files"
            return [
                "docs/integrations/clickpipes/home.mdx",
                "docs/integrations/language-clients/python/index.mdx",
            ]

    posted = {}

    def fake_post(comment_tags_and_bodies):
        posted.update(comment_tags_and_bodies)

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH, "post_updateable_comment", staticmethod(fake_post)
    )

    assert team_notifications.check()
    assert posted == {
        "team_notification": (
            "@ClickHouse/clickpipes @ClickHouse/integrations-ecosystem "
            "@ClickHouse/docs teams, please take a look"
        )
    }


def test_check_does_not_post_for_mixed_code_and_docs(monkeypatch):
    class FakeInfo:
        def get_kv_data(self, key):
            assert key == "changed_files"
            return ["docs/integrations/clickpipes/home.mdx", "src/Core/Block.cpp"]

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Mixed code and docs changes must not notify docs teams")

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)
    monkeypatch.setattr(
        team_notifications.GH,
        "post_updateable_comment",
        staticmethod(fail_if_called),
    )

    assert team_notifications.check()


def test_check_fails_when_changed_files_are_unavailable(monkeypatch):
    class FakeInfo:
        def get_kv_data(self, key):
            assert key == "changed_files"
            return None

    monkeypatch.setattr(team_notifications, "Info", FakeInfo)

    with pytest.raises(AssertionError, match="changed_files is not populated"):
        team_notifications.check()
