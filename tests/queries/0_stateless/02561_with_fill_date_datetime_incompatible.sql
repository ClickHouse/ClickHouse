SELECT today() AS a
ORDER BY a ASC WITH FILL FROM now() - toIntervalMonth(1) TO now() + toIntervalDay(1) STEP 82600; -- { serverError INVALID_WITH_FILL_EXPRESSION }
