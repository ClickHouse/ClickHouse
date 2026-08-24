SELECT (1, 2) IN ((1, (2, 3)), (1 + 1, 1)); -- { serverError TYPE_MISMATCH }
