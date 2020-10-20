-- If you are reading this test please note that as of now this setting does not provide benefits in most of the cases.
SET optimize_if_chain_to_multiif = 0;
EXPLAIN SYNTAX SELECT number = 1 ? 'hello' : (number = 2 ? 'world' : 'xyz') FROM numbers(10);
SET optimize_if_chain_to_multiif = 1;
EXPLAIN SYNTAX SELECT number = 1 ? 'hello' : (number = 2 ? 'world' : 'xyz') FROM numbers(10);
