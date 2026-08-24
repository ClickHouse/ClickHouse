SELECT bitmapTransform(arrayReduce('groupBitmapState', [1]), [1, 2], [1, 2, 3]); -- { serverError BAD_ARGUMENTS }
