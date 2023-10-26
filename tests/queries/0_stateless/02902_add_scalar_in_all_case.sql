SELECT * FROM format(TSVRaw, (SELECT '123', '456')) FORMAT TSVRaw -- { serverError 36 }
