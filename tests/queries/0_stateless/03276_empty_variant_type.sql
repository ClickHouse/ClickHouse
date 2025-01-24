set allow_experimental_variant_type=1;
create table test (v Variant()) engine=Variant(); -- {serverError BAD_ARGUMENTS}

