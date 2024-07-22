SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f']);

SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']);

SELECT arrayZip(); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }

SELECT arrayZip('a', 'b', 'c'); -- { serverError 43 }

SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']); -- { serverError 190 }
