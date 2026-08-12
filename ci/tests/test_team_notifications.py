import pytest

from ci.jobs.scripts.workflow_hooks import team_notifications


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
