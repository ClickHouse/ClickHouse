SELECT number
FROM numbers(10)
WHERE 1
SHUFFLE
SETTINGS
    allow_experimental_shuffle_query = 1,
    allow_experimental_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
