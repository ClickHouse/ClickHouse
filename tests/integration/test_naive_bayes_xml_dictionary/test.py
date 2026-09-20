"""A NAIVE_BAYES dictionary defined in an XML configuration file, with explicit priors written as
repeated `prior` elements. Covers the priors happy path (including whitespace around the values,
which is tolerated) and the validation of hand-written XML: an unexpected element inside `priors`
and a `prior` without a `probability` are both rejected when the dictionary loads."""

import pytest

from helpers.cluster import ClickHouseCluster

DICTIONARY_FILES = [
    "configs/dictionaries/nb_valid.xml",
    "configs/dictionaries/nb_unexpected_element.xml",
    "configs/dictionaries/nb_missing_probability.xml",
    "configs/dictionaries/nb_two_layouts.xml",
]

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", dictionaries=DICTIONARY_FILES)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        instance.query(
            """
            CREATE DATABASE IF NOT EXISTS test;
            CREATE TABLE test.nb_training (ngram String, class_id UInt32, count UInt64)
                ENGINE = MergeTree ORDER BY ngram;
            INSERT INTO test.nb_training VALUES ('good', 0, 5), ('bad', 1, 5);
            """
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_explicit_priors_from_xml(started_cluster):
    # An unseen token forms no known n-grams, so the class probabilities are exactly the priors.
    result = instance.query(
        "SELECT arrayMap(p -> (tupleElement(p, 1), round(tupleElement(p, 2), 4)),"
        " naiveBayesClassifierWithAllProbs('nb_valid', 'unseen'))"
    )
    assert result == "[(0,0.9),(1,0.1)]\n"

    # The priors take effect: 'bad' is a class-1 token, but the 0.9 prior for class 0 outweighs the
    # likelihood (0.9 * 1/7 > 0.1 * 6/7), so class 0 is predicted; uniform priors would predict 1.
    assert instance.query("SELECT dictGet('nb_valid', 'class_id', 'bad')") == "0\n"


def test_unexpected_element_in_priors(started_cluster):
    error = instance.query_and_get_error(
        "SELECT dictGet('nb_unexpected_element', 'class_id', 'good')"
    )
    assert "unexpected element 'entry' in priors, expected repeated 'prior' elements" in error


def test_prior_without_probability(started_cluster):
    error = instance.query_and_get_error(
        "SELECT dictGet('nb_missing_probability', 'class_id', 'good')"
    )
    assert "each prior must contain a 'class' id and a 'probability'" in error


def test_layout_with_two_children(started_cluster):
    # The layout element of this hand-written definition has two children. naiveBayesClassifier reads
    # the layout type from the configuration during query analysis, so the malformed definition is
    # reported there, with the same error loading the dictionary would produce.
    error = instance.query_and_get_error(
        "SELECT naiveBayesClassifier('nb_two_layouts', 'good')"
    )
    assert "element dictionary.layout should have exactly one child element" in error
