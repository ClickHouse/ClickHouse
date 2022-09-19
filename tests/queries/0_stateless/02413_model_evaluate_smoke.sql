-- This model does not exist:
SELECT modelEvaluate('hello', 1, 2, 3); -- { serverError 36 }
