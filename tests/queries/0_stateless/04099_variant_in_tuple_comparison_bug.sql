SELECT tuple('a'::Variant(String, UInt64)) = '(\'a\')';
SELECT tuple(materialize('a')::Variant(String, UInt64)) = '(\'a\')';

SELECT tuple('a'::Variant(String, UInt64)) = '(\'a\')' SETTINGS use_variant_default_implementation_for_comparisons = 0;
SELECT tuple(materialize('a')::Variant(String, UInt64)) = '(\'a\')' SETTINGS use_variant_default_implementation_for_comparisons = 0;

