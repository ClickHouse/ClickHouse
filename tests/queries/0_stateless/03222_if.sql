SELECT if(materialize(1), map(1, 2, 3, 4), map(3, 4, map(3, 4), 6)) settings use_variant_as_common_type = true, allow_experimental_variant_type = true;
SELECT if(materialize(1), map(1, 2, 3, 4), map(3, toLowCardinality(4), map(3, 4), 6)) settings use_variant_as_common_type = true, allow_experimental_variant_type = true;
