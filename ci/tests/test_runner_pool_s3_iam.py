"""
Runner-pool S3 IAM scoping.

`s3:ListBucket` is a bucket-level action, so granting it on the bucket ARN
without an `s3:prefix` condition lets a pool scoped to `bucket/PRs` enumerate
every key in the bucket. These tests lock the split (object-level vs
bucket-level) statement generation so that prefix-scoped access stays
prefix-scoped.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../ci"))

from praktika.infrastructure.native.runner_pool import _s3_grants, _s3_statements

RW_OBJECT = ["s3:GetObject", "s3:PutObject"]
RW_LIST = ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketLocation"]


def _list_statements(statements):
    return [s for s in statements if "s3:ListBucket" in s["Action"]]


def _object_statements(statements):
    return [s for s in statements if "s3:ListBucket" not in s["Action"]]


def test_prefix_scoped_listbucket_has_prefix_condition():
    statements = _s3_statements(
        ["clickhouse-builds/PRs"], "AllowedS3ReadWrite", RW_OBJECT, RW_LIST
    )
    list_stmts = _list_statements(statements)
    assert len(list_stmts) == 1
    stmt = list_stmts[0]
    # ListBucket is bucket-level: resource is the bucket ARN, never an object ARN.
    assert stmt["Resource"] == "arn:aws:s3:::clickhouse-builds"
    # ...and it must carry an s3:prefix condition so it cannot enumerate REFs/ccache.
    assert stmt["Condition"]["StringLike"]["s3:prefix"] == ["PRs", "PRs/*"]


def test_object_actions_only_on_object_arns():
    statements = _s3_statements(
        ["clickhouse-builds/PRs"], "AllowedS3ReadWrite", RW_OBJECT, RW_LIST
    )
    obj_stmts = _object_statements(statements)
    assert len(obj_stmts) == 1
    resources = obj_stmts[0]["Resource"]
    # Object statements never include the bare bucket ARN.
    assert "arn:aws:s3:::clickhouse-builds" not in resources
    assert resources == [
        "arn:aws:s3:::clickhouse-builds/PRs",
        "arn:aws:s3:::clickhouse-builds/PRs/*",
    ]


def test_no_unconstrained_listbucket_for_prefix_scope():
    # The regression guard: a prefix-scoped grant must never yield a ListBucket
    # statement on the bucket ARN without an s3:prefix condition.
    statements = _s3_statements(
        ["clickhouse-builds/PRs", "clickhouse-builds/REFs"],
        "AllowedS3ReadWrite",
        RW_OBJECT,
        RW_LIST,
    )
    for stmt in _list_statements(statements):
        assert "Condition" in stmt, f"unconstrained ListBucket: {stmt}"
    # Multiple prefixes on the same bucket merge into one condition set.
    (list_stmt,) = _list_statements(statements)
    assert list_stmt["Condition"]["StringLike"]["s3:prefix"] == [
        "PRs",
        "PRs/*",
        "REFs",
        "REFs/*",
    ]


def test_bucket_wide_grant_is_unconstrained():
    # A bucket-only allowance (or an explicit bucket ARN) intentionally lists the
    # whole bucket, so no s3:prefix condition is attached.
    for prefix in ("clickhouse-builds", "arn:aws:s3:::clickhouse-builds"):
        statements = _s3_statements([prefix], "AllowedS3ReadWrite", RW_OBJECT, RW_LIST)
        (list_stmt,) = _list_statements(statements)
        assert list_stmt["Resource"] == "arn:aws:s3:::clickhouse-builds"
        assert "Condition" not in list_stmt
        # ...and full object access under the bucket.
        (obj_stmt,) = _object_statements(statements)
        assert obj_stmt["Resource"] == ["arn:aws:s3:::clickhouse-builds/*"]


def test_explicit_object_arn_grants_no_listing():
    # An explicit object ARN grants object access but must not add ListBucket.
    _, bucket_conditions = _s3_grants(["arn:aws:s3:::other-bucket/exact/key"])
    assert bucket_conditions == {}
    statements = _s3_statements(
        ["arn:aws:s3:::other-bucket/exact/key"], "AllowedS3ReadWrite", RW_OBJECT, RW_LIST
    )
    assert _list_statements(statements) == []
