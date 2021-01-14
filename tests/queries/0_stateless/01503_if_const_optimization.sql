SELECT if(CAST(NULL), '2.55', NULL) AS x; -- { serverError 42 }
