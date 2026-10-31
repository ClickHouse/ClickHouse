-- `topK` of a floating point column counts the two zeros together and reports them as the positive zero,
-- regardless of the order of the input.
SELECT topK(1)(x), topKWeighted(1)(x, 1), approx_top_k(1)(x) FROM (SELECT arrayJoin([-0.::Float64, 0.::Float64]) AS x);
SELECT topK(1)(x), topKWeighted(1)(x, 1) FROM (SELECT arrayJoin([-0.::Float32, 0.::Float32]) AS x);
SELECT topK(1)(x), topKWeighted(1)(x, 1) FROM (SELECT arrayJoin([-0.::BFloat16, 0.::BFloat16]) AS x);
SELECT topK(1, 3, 'counts')(x) FROM (SELECT arrayJoin([-0., 0., -0., 1.]) AS x);
SELECT topKIf(1)(x, x <= 0) FROM (SELECT arrayJoin([-0., 0., 1.]) AS x);
SELECT topK(1)(toNullable(x)) FROM (SELECT arrayJoin([-0., 0.]) AS x);
SELECT topK(1)(x) FROM (SELECT -0. AS x FROM numbers(10));
