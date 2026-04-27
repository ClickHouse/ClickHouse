DROP TABLE IF EXISTS t_leading_zeroes;
DROP TABLE IF EXISTS t_leading_zeroes_f;

CREATE TABLE t_leading_zeroes(id Int64, input String, val Int64, expected Int64, comment String) ENGINE=MergeTree ORDER BY id;
CREATE TABLE t_leading_zeroes_f(id Int64, input String, val Float64, expected Float64, comment String) ENGINE=MergeTree ORDER BY id;

SET input_format_values_interpret_expressions = 0;

INSERT INTO t_leading_zeroes VALUES (1000, '0', 0, 0, 'Single zero');
INSERT INTO t_leading_zeroes VALUES (1001, '00', 00, 0, 'Double zero');
INSERT INTO t_leading_zeroes VALUES (1002, '000000000000000', 000000000000000, 0, 'Mutliple redundant zeroes');
INSERT INTO t_leading_zeroes VALUES (1003, '01', 01, 1, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes VALUES (1004, '08', 08, 8, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes VALUES (1005, '0100', 0100, 100, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes VALUES (1006, '0000000000100', 0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes');

INSERT INTO t_leading_zeroes VALUES (1010, '-0', -0, 0, 'Single zero negative');
INSERT INTO t_leading_zeroes VALUES (1011, '-00', -00, 0, 'Double zero negative');
INSERT INTO t_leading_zeroes VALUES (1012, '-000000000000000', -000000000000000, 0, 'Mutliple redundant zeroes negative');
INSERT INTO t_leading_zeroes VALUES (1013, '-01', -01, -1, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes VALUES (1014, '-08', -08, -8, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes VALUES (1015, '-0100', -0100, -100, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes VALUES (1016, '-0000000000100', -0000000000100, -100, 'Octal like, interpret as decimal, multiple leading zeroes negative');

INSERT INTO t_leading_zeroes VALUES (1020, '+0', +0, 0, 'Single zero positive');
INSERT INTO t_leading_zeroes VALUES (1021, '+00', +00, 0, 'Double zero negpositiveative');
INSERT INTO t_leading_zeroes VALUES (1022, '+000000000000000', +000000000000000, 0, 'Mutliple redundant zeroes positive');
INSERT INTO t_leading_zeroes VALUES (1023, '+01', +01, 1, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes VALUES (1024, '+08', +08, 8, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes VALUES (1025, '+0100', +0100, 100, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes VALUES (1026, '+0000000000100', +0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes positive');

INSERT INTO t_leading_zeroes VALUES (1030, '0000.008', 0000.008, 0, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1031, '-0000.008', -0000.008, 0, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1032, '+0000.008', +0000.008, 0, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1033, '0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1034, '-0000.008e3', -0000.008e3, -8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1035, '+0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1036, '08000.008e-3', 08000.008e-3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1037, '-08000.008e-3', -08000.008e-3, -8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (1038, '+08000.008e-3', 08000.008e-3, 8, 'Floating point should work...');

INSERT INTO t_leading_zeroes VALUES (1060, '0x0abcd', 0x0abcd, 43981, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1061, '-0x0abcd', -0x0abcd, -43981, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1062, '+0x0abcd', +0x0abcd, 43981, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1063, '0x0abcdP1', 0x0abcdP1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1064, '0x0abcdP+1', 0x0abcdP+1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1065, '0x0abcdP-1', 0x0abcdP-1, 21990, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1066, '0x0abcdP01', 0x0abcdP01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1067, '0x0abcdP+01', 0x0abcdP+01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (1068, '0x0abcdP-01', 0x0abcdP-01, 21990, 'Hex should be parsed');


-- Floating point numbers go via readFloatTextFastImpl - so should not be affected

INSERT INTO t_leading_zeroes_f VALUES (2000, '0', 0, 0, 'Single zero');
INSERT INTO t_leading_zeroes_f VALUES (2001, '00', 00, 0, 'Double zero');
INSERT INTO t_leading_zeroes_f VALUES (2002, '000000000000000', 000000000000000, 0, 'Mutliple redundant zeroes');
INSERT INTO t_leading_zeroes_f VALUES (2003, '01', 01, 1, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes_f VALUES (2004, '08', 08, 8, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes_f VALUES (2005, '0100', 0100, 100, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes_f VALUES (2006, '0000000000100', 0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes');

-- Float negative zero is machine/context dependent
-- INSERT INTO t_leading_zeroes_f VALUES (2010, '-0', -0, 0, 'Single zero negative');
-- INSERT INTO t_leading_zeroes_f VALUES (2011, '-00', -00, 0, 'Double zero negative');
-- INSERT INTO t_leading_zeroes_f VALUES (2012, '-000000000000000', -000000000000000, 0, 'Mutliple redundant zeroes negative');
INSERT INTO t_leading_zeroes_f VALUES (2013, '-01', -01, -1, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes_f VALUES (2014, '-08', -08, -8, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes_f VALUES (2015, '-0100', -0100, -100, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes_f VALUES (2016, '-0000000000100', -0000000000100, -100, 'Octal like, interpret as decimal, multiple leading zeroes negative');

INSERT INTO t_leading_zeroes_f VALUES (2020, '+0', +0, 0, 'Single zero positive');
INSERT INTO t_leading_zeroes_f VALUES (2021, '+00', +00, 0, 'Double zero negpositiveative');
INSERT INTO t_leading_zeroes_f VALUES (2022, '+000000000000000', +000000000000000, 0, 'Mutliple redundant zeroes positive');
INSERT INTO t_leading_zeroes_f VALUES (2023, '+01', +01, 1, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes_f VALUES (2024, '+08', +08, 8, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes_f VALUES (2025, '+0100', +0100, 100, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes_f VALUES (2026, '+0000000000100', +0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes positive');

INSERT INTO t_leading_zeroes_f VALUES (2030, '0000.008', 0000.008, 0.008, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2031, '-0000.008', -0000.008, -0.008, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2032, '+0000.008', +0000.008, 0.008, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2033, '0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2034, '-0000.008e3', -0000.008e3, -8, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2035, '+0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2036, '08.5e-3', 08.5e-3, 0.0085, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2037, '-08.5e-3', -08.5e-3, -0.0085, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (2038, '+08.5e-3', 08.5e-3, 0.0085, 'Floating point should work...');

INSERT INTO t_leading_zeroes_f VALUES (2063, '0x0abcdP1', 0x0abcdP1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (2064, '0x0abcdP+1', 0x0abcdP+1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (2065, '0x0abcdP-1', 0x0abcdP-1, 21990.5, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (2066, '0x0abcdP01', 0x0abcdP01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (2067, '0x0abcdP+01', 0x0abcdP+01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (2068, '0x0abcdP-01', 0x0abcdP-01, 21990.5, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (2069, '0x01P-01', 0x01P-01, 0.5, 'Hex should be parsed');

-- Coincidentally, the following result in 9 rather than 9e9 because of readFloatTextFastImpl
-- using readUIntTextUpToNSignificantDigits<4>(exponent, in)
-- INSERT INTO t_leading_zeroes_f VALUES (2070, '00009e00009', 00009e00009, 9e9, '???');

-- Binary should not work with input_format_values_interpret_expressions = 0

INSERT INTO t_leading_zeroes_f VALUES (2050, '0b10000', 0b10000, 16, 'Binary should not be parsed'); -- { error SYNTAX_ERROR }
INSERT INTO t_leading_zeroes_f VALUES (2051, '-0b10000', -0b10000, -16, 'Binary should not be parsed'); -- { error SYNTAX_ERROR }
INSERT INTO t_leading_zeroes_f VALUES (2052, '+0b10000', +0b10000, 16, 'Binary should not be parsed'); -- { error SYNTAX_ERROR }

INSERT INTO t_leading_zeroes VALUES (1050, '0b10000', 0b10000, 16, 'Binary should not be parsed'); -- { error SYNTAX_ERROR }
INSERT INTO t_leading_zeroes VALUES (1051, '-0b10000', -0b10000, -16, 'Binary should not be parsed'); -- { error SYNTAX_ERROR }
INSERT INTO t_leading_zeroes VALUES (1052, '+0b10000', +0b10000, 16, 'Binary should not be parsed'); -- { error SYNTAX_ERROR }



SET input_format_values_interpret_expressions = 1;

INSERT INTO t_leading_zeroes VALUES (11000, '0', 0, 0, 'Single zero');
INSERT INTO t_leading_zeroes VALUES (11001, '00', 00, 0, 'Double zero');
INSERT INTO t_leading_zeroes VALUES (11002, '000000000000000', 000000000000000, 0, 'Mutliple redundant zeroes');
INSERT INTO t_leading_zeroes VALUES (11003, '01', 01, 1, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes VALUES (11004, '08', 08, 8, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes VALUES (11005, '0100', 0100, 100, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes VALUES (11006, '0000000000100', 0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes');

INSERT INTO t_leading_zeroes VALUES (11010, '-0', -0, 0, 'Single zero negative');
INSERT INTO t_leading_zeroes VALUES (11011, '-00', -00, 0, 'Double zero negative');
INSERT INTO t_leading_zeroes VALUES (11012, '-000000000000000', -000000000000000, 0, 'Mutliple redundant zeroes negative');
INSERT INTO t_leading_zeroes VALUES (11013, '-01', -01, -1, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes VALUES (11014, '-08', -08, -8, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes VALUES (11015, '-0100', -0100, -100, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes VALUES (11016, '-0000000000100', -0000000000100, -100, 'Octal like, interpret as decimal, multiple leading zeroes negative');

INSERT INTO t_leading_zeroes VALUES (11020, '+0', +0, 0, 'Single zero positive');
INSERT INTO t_leading_zeroes VALUES (11021, '+00', +00, 0, 'Double zero negpositiveative');
INSERT INTO t_leading_zeroes VALUES (11022, '+000000000000000', +000000000000000, 0, 'Mutliple redundant zeroes positive');
INSERT INTO t_leading_zeroes VALUES (11023, '+01', +01, 1, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes VALUES (11024, '+08', +08, 8, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes VALUES (11025, '+0100', +0100, 100, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes VALUES (11026, '+0000000000100', +0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes positive');

INSERT INTO t_leading_zeroes VALUES (11030, '0000.008', 0000.008, 0, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11031, '-0000.008', -0000.008, 0, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11032, '+0000.008', +0000.008, 0, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11033, '0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11034, '-0000.008e3', -0000.008e3, -8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11035, '+0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11036, '08000.008e-3', 08000.008e-3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11037, '-08000.008e-3', -08000.008e-3, -8, 'Floating point should work...');
INSERT INTO t_leading_zeroes VALUES (11038, '+08000.008e-3', 08000.008e-3, 8, 'Floating point should work...');

INSERT INTO t_leading_zeroes VALUES (11050, '0b10000', 0b10000, 16, 'Binary should be parsed');
INSERT INTO t_leading_zeroes VALUES (11051, '-0b10000', -0b10000, -16, 'Binary should be parsed');
INSERT INTO t_leading_zeroes VALUES (11052, '+0b10000', +0b10000, 16, 'Binary should be parsed');

INSERT INTO t_leading_zeroes VALUES (11060, '0x0abcd', 0x0abcd, 43981, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11061, '-0x0abcd', -0x0abcd, -43981, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11062, '+0x0abcd', +0x0abcd, 43981, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11063, '0x0abcdP1', 0x0abcdP1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11064, '0x0abcdP+1', 0x0abcdP+1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11065, '0x0abcdP-1', 0x0abcdP-1, 21990, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11066, '0x0abcdP01', 0x0abcdP01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11067, '0x0abcdP+01', 0x0abcdP+01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes VALUES (11068, '0x0abcdP-01', 0x0abcdP-01, 21990, 'Hex should be parsed');

-- Floating point numbers go via readFloatTextFastImpl - so should not be affected

INSERT INTO t_leading_zeroes_f VALUES (12000, '0', 0, 0, 'Single zero');
INSERT INTO t_leading_zeroes_f VALUES (12001, '00', 00, 0, 'Double zero');
INSERT INTO t_leading_zeroes_f VALUES (12002, '000000000000000', 000000000000000, 0, 'Mutliple redundant zeroes');
INSERT INTO t_leading_zeroes_f VALUES (12003, '01', 01, 1, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes_f VALUES (12004, '08', 08, 8, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes_f VALUES (12005, '0100', 0100, 100, 'Octal like, interpret as decimal');
INSERT INTO t_leading_zeroes_f VALUES (12006, '0000000000100', 0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes');

-- Float negative zero is machine/context dependent
-- INSERT INTO t_leading_zeroes_f VALUES (12010, '-0', -0, 0, 'Single zero negative');
-- INSERT INTO t_leading_zeroes_f VALUES (12011, '-00', -00, 0, 'Double zero negative');
-- INSERT INTO t_leading_zeroes_f VALUES (12012, '-000000000000000', -000000000000000, 0, 'Mutliple redundant zeroes negative');
INSERT INTO t_leading_zeroes_f VALUES (12013, '-01', -01, -1, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes_f VALUES (12014, '-08', -08, -8, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes_f VALUES (12015, '-0100', -0100, -100, 'Octal like, interpret as decimal negative');
INSERT INTO t_leading_zeroes_f VALUES (12016, '-0000000000100', -0000000000100, -100, 'Octal like, interpret as decimal, multiple leading zeroes negative');

INSERT INTO t_leading_zeroes_f VALUES (12020, '+0', +0, 0, 'Single zero positive');
INSERT INTO t_leading_zeroes_f VALUES (12021, '+00', +00, 0, 'Double zero negpositiveative');
INSERT INTO t_leading_zeroes_f VALUES (12022, '+000000000000000', +000000000000000, 0, 'Mutliple redundant zeroes positive');
INSERT INTO t_leading_zeroes_f VALUES (12023, '+01', +01, 1, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes_f VALUES (12024, '+08', +08, 8, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes_f VALUES (12025, '+0100', +0100, 100, 'Octal like, interpret as decimal positive');
INSERT INTO t_leading_zeroes_f VALUES (12026, '+0000000000100', +0000000000100, 100, 'Octal like, interpret as decimal, multiple leading zeroes positive');

INSERT INTO t_leading_zeroes_f VALUES (12030, '0000.008', 0000.008, 0.008, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12031, '-0000.008', -0000.008, -0.008, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12032, '+0000.008', +0000.008, 0.008, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12033, '0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12034, '-0000.008e3', -0000.008e3, -8, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12035, '+0000.008e3', 0000.008e3, 8, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12036, '08.5e-3', 08.5e-3, 0.0085, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12037, '-08.5e-3', -08.5e-3, -0.0085, 'Floating point should work...');
INSERT INTO t_leading_zeroes_f VALUES (12038, '+08.5e-3', 08.5e-3, 0.0085, 'Floating point should work...');

INSERT INTO t_leading_zeroes_f VALUES (12050, '0b10000', 0b10000, 16, 'Binary should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12051, '-0b10000', -0b10000, -16, 'Binary should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12052, '+0b10000', +0b10000, 16, 'Binary should be parsed');

INSERT INTO t_leading_zeroes_f VALUES (12063, '0x0abcdP1', 0x0abcdP1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12064, '0x0abcdP+1', 0x0abcdP+1, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12065, '0x0abcdP-1', 0x0abcdP-1, 21990.5, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12066, '0x0abcdP01', 0x0abcdP01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12067, '0x0abcdP+01', 0x0abcdP+01, 87962, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12068, '0x0abcdP-01', 0x0abcdP-01, 21990.5, 'Hex should be parsed');
INSERT INTO t_leading_zeroes_f VALUES (12069, '0x01P-01', 0x01P-01, 0.5, 'Hex should be parsed');

SELECT 'Leading zeroes into Int64 (1XXX without input_format_values_interpret_expressions and 1XXXX with)';
SELECT t.val == t.expected AS ok, * FROM t_leading_zeroes t ORDER BY id;


SELECT 'Leading zeroes into Float64 (2XXX without input_format_values_interpret_expressions and 2XXXX with)';
SELECT t.val == t.expected AS ok, * FROM t_leading_zeroes_f t ORDER BY id;


DROP TABLE IF EXISTS t_leading_zeroes;
DROP TABLE IF EXISTS t_leading_zeroes_f;
