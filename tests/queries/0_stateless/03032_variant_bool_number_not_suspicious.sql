set allow_experimental_variant_type=1;
set allow_suspicious_variant_types=0;
select 'true'::Bool::Variant(UInt32, Bool);

