-- This query throw high-level exception instead of low-level "too large size passed to allocator":

SELECT * FROM format(CSV, 'entitypArray AggregateFunction(groupArray, String)',
'295TMiews.viewNÿÿÿÿÿ""""""TabSeparÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿÿated
d St""


r'); -- { serverError TOO_LARGE_ARRAY_SIZE }
