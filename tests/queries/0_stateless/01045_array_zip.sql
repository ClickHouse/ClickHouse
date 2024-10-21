SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f']);

SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']);

SELECT arrayZip(); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }

SELECT arrayZip('a', 'b', 'c'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT arrayZip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
