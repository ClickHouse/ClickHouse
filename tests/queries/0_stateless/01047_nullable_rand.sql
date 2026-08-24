select toTypeName(rand(cast(4 as Nullable(UInt8))));
select toTypeName(randCanonical(CAST(4 as Nullable(UInt8))));
select toTypeName(randConstant(CAST(4 as Nullable(UInt8))));
select toTypeName(rand(Null));
select toTypeName(randCanonical(Null));
select toTypeName(randConstant(Null));

select rand(cast(4 as Nullable(UInt8))) * 0;
select randCanonical(cast(4 as Nullable(UInt8))) * 0;
select randConstant(CAST(4 as Nullable(UInt8))) * 0;
select rand(Null) * 0;
select randCanonical(Null) * 0;
select randConstant(Null) * 0;
