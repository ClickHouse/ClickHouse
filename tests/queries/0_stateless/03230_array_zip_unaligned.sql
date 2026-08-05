SELECT arrayZipUnaligned(['a', 'b', 'c'], ['d', 'e', 'f']) as x, toTypeName(x);

SELECT arrayZipUnaligned(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']);

SELECT arrayZipUnaligned();

SELECT arrayZipUnaligned('a', 'b', 'c'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT arrayZipUnaligned(['a', 'b', 'c'], ['d', 'e', 'f', 'g']);

SELECT arrayZipUnaligned(['a'], [1, 2, 3]);

SELECT arrayZipUnaligned(['a', 'b', 'c'], [1, 2], [1.1, 2.2, 3.3, 4.4]);

SELECT arrayZipUnaligned(materialize(['g', 'h', 'i'])) from numbers(3); 
