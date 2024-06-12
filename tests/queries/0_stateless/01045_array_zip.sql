SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f']);

SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']);

SELECT arrayZip(); -- { serverError 42 }

SELECT arrayZip('a', 'b', 'c'); -- { serverError 43 }

SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']); -- { serverError 190 }
