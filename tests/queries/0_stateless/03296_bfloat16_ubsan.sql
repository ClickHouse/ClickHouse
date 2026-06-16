SET allow_experimental_bfloat16_type = 1;
SELECT (65535::BFloat16)::Int16; -- The result is implementation defined on overflow.
