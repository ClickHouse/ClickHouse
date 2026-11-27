-- Tags: no-fasttest
-- no-fasttest: Arrow format is not available in the fasttest environment

-- Reading several Arrow files under one glob infers the header from one of them and realigns the
-- others to it, so a zero-element struct in the header meets a non-empty struct of the same name.
-- The two files carry the shapes swapped and the check is an aggregate because the glob listing
-- order is not deterministic: whichever file supplies the header, one of `a`/`b` is empty there.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05052_1.arrow', 'Arrow', 'n Int32, a Tuple(), b Tuple(x Int32)')
SELECT number::Int32, tuple(), tuple(number::Int32) FROM numbers(3);

INSERT INTO FUNCTION file(currentDatabase() || '_05052_2.arrow', 'Arrow', 'n Int32, a Tuple(x Int32), b Tuple()')
SELECT (number + 10)::Int32, tuple(number::Int32), tuple() FROM numbers(3);

SELECT count(), sum(n), sum(ignore(a, b)) FROM file(currentDatabase() || '_05052_{1,2}.arrow', 'Arrow');

SELECT * FROM file(currentDatabase() || '_05052_1.arrow', 'Arrow') ORDER BY n;
SELECT * FROM file(currentDatabase() || '_05052_2.arrow', 'Arrow') ORDER BY n;
