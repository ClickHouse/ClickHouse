set enable_variant_type=1;

select 't'::Variant(String, Bool) as v, variantType(v);
select 'on'::Variant(String, Bool) as v, variantType(v);
select 'f'::Variant(String, Bool) as v, variantType(v);
select 'off'::Variant(String, Bool) as v, variantType(v);
select 'true'::Variant(String, Bool) as v, variantType(v);
select 'false'::Variant(String, Bool) as v, variantType(v);

set allow_special_bool_values_inside_variant=1;
select 't'::Variant(String, Bool) as v, variantType(v);
select 'on'::Variant(String, Bool) as v, variantType(v);
select 'f'::Variant(String, Bool) as v, variantType(v);
select 'off'::Variant(String, Bool) as v, variantType(v);
select 'true'::Variant(String, Bool) as v, variantType(v);
select 'false'::Variant(String, Bool) as v, variantType(v);

set allow_special_bool_values_inside_variant=0;
set cast_string_to_variant_use_inference=0;
select 't'::Variant(String, Bool) as v, variantType(v);
select 'on'::Variant(String, Bool) as v, variantType(v);
select 'f'::Variant(String, Bool) as v, variantType(v);
select 'off'::Variant(String, Bool) as v, variantType(v);
select 'true'::Variant(String, Bool) as v, variantType(v);
select 'false'::Variant(String, Bool) as v, variantType(v);

set allow_special_bool_values_inside_variant=1;
select 't'::Variant(String, Bool) as v, variantType(v);
select 'on'::Variant(String, Bool) as v, variantType(v);
select 'f'::Variant(String, Bool) as v, variantType(v);
select 'off'::Variant(String, Bool) as v, variantType(v);
select 'true'::Variant(String, Bool) as v, variantType(v);
select 'false'::Variant(String, Bool) as v, variantType(v);


