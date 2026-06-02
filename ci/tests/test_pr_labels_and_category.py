import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import (
    get_category_with_label_fallback,
    infer_category_from_labels,
)


def test_get_category_with_label_fallback_uses_existing_category_label():
    error, category = get_category_with_label_fallback("", ["pr-bugfix", "can be tested"])
    assert error == ""
    assert category == "Bug Fix"


def test_get_category_with_label_fallback_still_fails_without_category_label():
    error, category = get_category_with_label_fallback("", ["can be tested"])
    assert error == "Change category is missing or invalid"
    assert category == ""


def test_get_category_with_label_fallback_reports_multiple_category_labels():
    error, category = get_category_with_label_fallback(
        "", ["pr-bugfix", "pr-feature", "can be tested"]
    )
    assert error == "More than one category label found for fallback: pr-bugfix, pr-feature"
    assert category == ""


def test_infer_category_from_labels_prefers_single_known_label():
    category, label, error = infer_category_from_labels(["foo", "pr-ci"])
    assert error == ""
    assert label == "pr-ci"
    assert category == "CI Fix or Improvement (changelog entry is not required)"
