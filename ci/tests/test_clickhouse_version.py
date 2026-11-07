"""
Tests for the `ClickHouseVersion` hash/eq contract
(`ci/jobs/scripts/clickhouse_version.py`).

`ClickHouseVersion` objects end up as set members and dict keys:
`get_supported_versions` unions two sets of them, and
`ci/tools/official_docker.py` dedupes with a set comprehension and keys a
dict by them. Python requires objects that compare equal to hash equal, so
`__hash__` must cover exactly the fields `__eq__` compares (major, minor,
patch, tweak) - not the release metadata `__eq__` ignores (revision,
githash, version type). A version parsed from a tag and the same version
built by the release flow differ in that metadata while being the same
release; if they hash differently, dedupe and lookup silently miss.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.clickhouse_version import ClickHouseVersion


def _version(**overrides):
    fields = dict(
        major=25,
        minor=9,
        patch=1,
        revision=100,
        tweak=123,
        githash="aaaaaaa",
        version_type="stable",
    )
    fields.update(overrides)
    return ClickHouseVersion(**fields)


class TestHashEqContract:
    def test_separately_built_copies_hash_equal(self):
        assert hash(_version()) == hash(_version())

    def test_metadata_that_eq_ignores_does_not_change_the_hash(self):
        released = _version()
        from_tag = _version(revision=999, githash="bbbbbbb", version_type="testing")
        assert released == from_tag
        assert hash(released) == hash(from_tag)

    def test_a_set_collapses_equal_versions(self):
        assert len({_version(), _version(version_type="lts")}) == 1

    def test_dict_lookup_by_an_equal_copy(self):
        versions = {_version(): "25.9.1.123"}
        assert versions[_version(githash="ccccccc")] == "25.9.1.123"

    def test_different_versions_stay_distinct(self):
        assert _version() != _version(tweak=124)
        assert len({_version(), _version(tweak=124)}) == 2
