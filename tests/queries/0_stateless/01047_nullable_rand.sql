select toTypeName(rand(cast(4 as Nullable(UInt8))));
select toTypeName(canonicalRand(CAST(4 as Nullable(UInt8))));
select toTypeName(randConstant(CAST(4 as Nullable(UInt8))));
select toTypeName(rand(Null));
select toTypeName(canonicalRand(Null));
select toTypeName(randConstant(Null));

select rand(cast(4 as Nullable(UInt8))) * 0;
select canonicalRand(cast(4 as Nullable(UInt8))) * 0;
select randConstant(CAST(4 as Nullable(UInt8))) * 0;
select rand(Null) * 0;
select canonicalRand(Null) * 0;
select randConstant(Null) * 0;
