SELECT 'Negative test of overlay';
SELECT overlay('hello', 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT overlay('hello', 'world'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT overlay('hello', 'world', 2, 3, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT overlay(123, 'world', 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT overlay('hello', 456, 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT overlay('hello', 'world', 'two', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT overlay('hello', 'world', 2, 'three'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Positive test 1 with various combinations of const/non-const columns';
SELECT overlay('Spark SQL', 'ANSI ', 7, 0), overlayUTF8('Spark SQL和CH', 'ANSI ', 7, 0);
SELECT overlay(materialize('Spark SQL'), 'ANSI ', 7, 0), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', 7, 0);
SELECT overlay('Spark SQL', materialize('ANSI '), 7, 0), overlayUTF8('Spark SQL和CH', materialize('ANSI '), 7, 0);
SELECT overlay('Spark SQL', 'ANSI ', materialize(7), 0), overlayUTF8('Spark SQL和CH', 'ANSI ', materialize(7), 0);
SELECT overlay('Spark SQL', 'ANSI ', 7, materialize(0)), overlayUTF8('Spark SQL和CH', 'ANSI ', 7, materialize(0));
SELECT overlay(materialize('Spark SQL'), materialize('ANSI '), 7, 0), overlayUTF8(materialize('Spark SQL和CH'), materialize('ANSI '), 7, 0);
SELECT overlay(materialize('Spark SQL'), 'ANSI ', materialize(7), 0), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', materialize(7), 0);
SELECT overlay(materialize('Spark SQL'), 'ANSI ', 7, materialize(0)), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', 7, materialize(0));
SELECT overlay('Spark SQL', materialize('ANSI '), materialize(7), 0), overlayUTF8('Spark SQL和CH', materialize('ANSI '), materialize(7), 0);
SELECT overlay('Spark SQL', materialize('ANSI '), 7, materialize(0)), overlayUTF8('Spark SQL和CH', materialize('ANSI '), 7, materialize(0));
SELECT overlay('Spark SQL', 'ANSI ', materialize(7), materialize(0)), overlayUTF8('Spark SQL和CH', 'ANSI ', materialize(7), materialize(0));
SELECT overlay(materialize('Spark SQL'), materialize('ANSI '), materialize(7), 0), overlayUTF8(materialize('Spark SQL和CH'), materialize('ANSI '), materialize(7), 0);
SELECT overlay(materialize('Spark SQL'), materialize('ANSI '), 7, materialize(0)), overlayUTF8(materialize('Spark SQL和CH'), materialize('ANSI '), 7, materialize(0));
SELECT overlay(materialize('Spark SQL'), 'ANSI ', materialize(7), materialize(0)), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', materialize(7), materialize(0));
SELECT overlay('Spark SQL', materialize('ANSI '), materialize(7), materialize(0)), overlayUTF8('Spark SQL和CH', materialize('ANSI '), materialize(7), materialize(0));
SELECT overlay(materialize('Spark SQL'), materialize('ANSI '), materialize(7), materialize(0)), overlayUTF8(materialize('Spark SQL和CH'), materialize('ANSI '), materialize(7), materialize(0));
	
SELECT 'Positive test 2 with various combinations of const/non-const columns';
SELECT overlay('Spark SQL', '_', 6), overlayUTF8('Spark SQL和CH', '_', 6);
SELECT overlay(materialize('Spark SQL'), '_', 6), overlayUTF8(materialize('Spark SQL和CH'), '_', 6);
SELECT overlay('Spark SQL', materialize('_'), 6), overlayUTF8('Spark SQL和CH', materialize('_'), 6);
SELECT overlay('Spark SQL', '_', materialize(6)), overlayUTF8('Spark SQL和CH', '_', materialize(6));
SELECT overlay(materialize('Spark SQL'), materialize('_'), 6), overlayUTF8(materialize('Spark SQL和CH'), materialize('_'), 6);
SELECT overlay(materialize('Spark SQL'), '_', materialize(6)), overlayUTF8(materialize('Spark SQL和CH'), '_', materialize(6));
SELECT overlay('Spark SQL', materialize('_'), materialize(6)), overlayUTF8('Spark SQL和CH', materialize('_'), materialize(6));
SELECT overlay(materialize('Spark SQL'), materialize('_'), materialize(6)), overlayUTF8(materialize('Spark SQL和CH'), materialize('_'), materialize(6));
	
SELECT 'Positive test 3 with various combinations of const/non-const columns';
SELECT overlay('Spark SQL', 'CORE', 7), overlayUTF8('Spark SQL和CH', 'CORE', 7);
SELECT overlay(materialize('Spark SQL'), 'CORE', 7), overlayUTF8(materialize('Spark SQL和CH'), 'CORE', 7);
SELECT overlay('Spark SQL', materialize('CORE'), 7), overlayUTF8('Spark SQL和CH', materialize('CORE'), 7);
SELECT overlay('Spark SQL', 'CORE', materialize(7)), overlayUTF8('Spark SQL和CH', 'CORE', materialize(7));
SELECT overlay(materialize('Spark SQL'), materialize('CORE'), 7), overlayUTF8(materialize('Spark SQL和CH'), materialize('CORE'), 7);
SELECT overlay(materialize('Spark SQL'), 'CORE', materialize(7)), overlayUTF8(materialize('Spark SQL和CH'), 'CORE', materialize(7));
SELECT overlay('Spark SQL', materialize('CORE'), materialize(7)), overlayUTF8('Spark SQL和CH', materialize('CORE'), materialize(7));
SELECT overlay(materialize('Spark SQL'), materialize('CORE'), materialize(7)), overlayUTF8(materialize('Spark SQL和CH'), materialize('CORE'), materialize(7));
	
SELECT 'Positive test 4 with various combinations of const/non-const columns';
SELECT overlay('Spark SQL', 'ANSI ', 7, 0), overlayUTF8('Spark SQL和CH', 'ANSI ', 7, 0);
SELECT overlay(materialize('Spark SQL'), 'ANSI ', 7, 0), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', 7, 0);
SELECT overlay('Spark SQL', materialize('ANSI '), 7, 0), overlayUTF8('Spark SQL和CH', materialize('ANSI '), 7, 0);
SELECT overlay('Spark SQL', 'ANSI ', materialize(7), 0), overlayUTF8('Spark SQL和CH', 'ANSI ', materialize(7), 0);
SELECT overlay('Spark SQL', 'ANSI ', 7, materialize(0)), overlayUTF8('Spark SQL和CH', 'ANSI ', 7, materialize(0));
SELECT overlay(materialize('Spark SQL'), materialize('ANSI '), 7, 0), overlayUTF8(materialize('Spark SQL和CH'), materialize('ANSI '), 7, 0);
SELECT overlay(materialize('Spark SQL'), 'ANSI ', materialize(7), 0), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', materialize(7), 0);
SELECT overlay(materialize('Spark SQL'), 'ANSI ', 7, materialize(0)), overlayUTF8(materialize('Spark SQL和CH'), 'ANSI ', 7, materialize(0));
SELECT overlay('Spark SQL', materialize('ANSI '), materialize(7), 0), overlayUTF8('Spark SQL和CH', materialize('ANSI '), materialize(7), 0);
SELECT overlay('Spark SQL', materialize('ANSI '), 7, materialize(0)), overlayUTF8('Spark SQL和CH', materialize('ANSI '), 7, materialize(0));
SELECT overlay('Spark SQL', 'ANSI ', materialize(7), materialize(0)), overlayUTF8('Spark SQL和CH', 'ANSI ', materialize(7), materialize(0));
SELECT overlay(materialize('Spark SQL'), materialize('ANSI '), materialize(7), materialize(0)), overlayUTF8(materialize('Spark SQL和CH'), materialize('ANSI '), materialize(7), materialize(0));
	
SELECT 'Positive test 5 with various combinations of const/non-const columns';
SELECT overlay('Spark SQL', 'tructured', 2, 4), overlayUTF8('Spark SQL和CH', 'tructured', 2, 4);
SELECT overlay(materialize('Spark SQL'), 'tructured', 2, 4), overlayUTF8(materialize('Spark SQL和CH'), 'tructured', 2, 4);
SELECT overlay('Spark SQL', materialize('tructured'), 2, 4), overlayUTF8('Spark SQL和CH', materialize('tructured'), 2, 4);
SELECT overlay('Spark SQL', 'tructured', materialize(2), 4), overlayUTF8('Spark SQL和CH', 'tructured', materialize(2), 4);
SELECT overlay('Spark SQL', 'tructured', 2, materialize(4)), overlayUTF8('Spark SQL和CH', 'tructured', 2, materialize(4));
SELECT overlay(materialize('Spark SQL'), materialize('tructured'), 2, 4), overlayUTF8(materialize('Spark SQL和CH'), materialize('tructured'), 2, 4);
SELECT overlay(materialize('Spark SQL'), 'tructured', materialize(2), 4), overlayUTF8(materialize('Spark SQL和CH'), 'tructured', materialize(2), 4);
SELECT overlay(materialize('Spark SQL'), 'tructured', 2, materialize(4)), overlayUTF8(materialize('Spark SQL和CH'), 'tructured', 2, materialize(4));
SELECT overlay('Spark SQL', materialize('tructured'), materialize(2), 4), overlayUTF8('Spark SQL和CH', materialize('tructured'), materialize(2), 4);
SELECT overlay('Spark SQL', materialize('tructured'), 2, materialize(4)), overlayUTF8('Spark SQL和CH', materialize('tructured'), 2, materialize(4));
SELECT overlay('Spark SQL', 'tructured', materialize(2), materialize(4)), overlayUTF8('Spark SQL和CH', 'tructured', materialize(2), materialize(4));
SELECT overlay(materialize('Spark SQL'), materialize('tructured'), materialize(2), materialize(4)), overlayUTF8(materialize('Spark SQL和CH'), materialize('tructured'), materialize(2), materialize(4));
