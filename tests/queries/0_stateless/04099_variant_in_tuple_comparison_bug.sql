SELECT tuple('a'::Variant(String, UInt64)) = '(\'a\')';
SELECT tuple(materialize('a')::Variant(String, UInt64)) = '(\'a\')';

