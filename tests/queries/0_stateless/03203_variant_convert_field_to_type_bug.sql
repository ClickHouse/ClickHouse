set use_variant_as_common_type=1;

SELECT * FROM numbers([tuple(1, 2), NULL], 2); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

