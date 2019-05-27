drop table if exists default.decimals;

create table default.decimals (key UInt64, d32 Decimal32(4), d64 Decimal64(6), d128 Decimal128(1)) Engine = Memory;
insert into default.decimals (key, d32, d64, d128) values (1, 1, 1, 1);

select 'dictGet(flat)', toUInt64(1) as k,
    dictGet('flat_decimals', 'd32', k),
    dictGet('flat_decimals', 'd64', k),
    dictGet('flat_decimals', 'd128', k)
    from default.decimals;
select 'dictGetOrDefault(flat)', toUInt64(1) as k,
    dictGetOrDefault('flat_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('flat_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('flat_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;
select 'dictGetOrDefault(flat)', toUInt64(0) as k,
    dictGetOrDefault('flat_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('flat_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('flat_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;

select 'dictGet(hashed)', toUInt64(1) as k,
    dictGet('hashed_decimals', 'd32', k),
    dictGet('hashed_decimals', 'd64', k),
    dictGet('hashed_decimals', 'd128', k)
    from default.decimals;
select 'dictGetOrDefault(hashed)', toUInt64(1) as k,
    dictGetOrDefault('hashed_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('hashed_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('hashed_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;
select 'dictGetOrDefault(hashed)', toUInt64(0) as k,
    dictGetOrDefault('hashed_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('hashed_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('hashed_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;

select 'dictGet(cache)', toUInt64(1) as k,
    dictGet('cache_decimals', 'd32', k),
    dictGet('cache_decimals', 'd64', k),
    dictGet('cache_decimals', 'd128', k)
    from default.decimals;
select 'dictGetOrDefault(cache)', toUInt64(1) as k,
    dictGetOrDefault('cache_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('cache_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('cache_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;
select 'dictGetOrDefault(cache)', toUInt64(0) as k,
    dictGetOrDefault('cache_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('cache_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('cache_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;

--

select 'dictGet(hashed)', tuple(toUInt64(1)) as k,
    dictGet('complex_hashed_decimals', 'd32', k),
    dictGet('complex_hashed_decimals', 'd64', k),
    dictGet('complex_hashed_decimals', 'd128', k)
    from default.decimals;
select 'dictGetOrDefault(hashed)', tuple(toUInt64(1)) as k,
    dictGetOrDefault('complex_hashed_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('complex_hashed_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('complex_hashed_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;
select 'dictGetOrDefault(hashed)', tuple(toUInt64(0)) as k,
    dictGetOrDefault('complex_hashed_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('complex_hashed_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('complex_hashed_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;

select 'dictGet(cache)', tuple(toUInt64(1)) as k,
    dictGet('complex_cache_decimals', 'd32', k),
    dictGet('complex_cache_decimals', 'd64', k),
    dictGet('complex_cache_decimals', 'd128', k)
    from default.decimals;
select 'dictGetOrDefault(cache)', tuple(toUInt64(1)) as k,
    dictGetOrDefault('complex_cache_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('complex_cache_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('complex_cache_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;
select 'dictGetOrDefault(cache)', tuple(toUInt64(0)) as k,
    dictGetOrDefault('complex_cache_decimals', 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault('complex_cache_decimals', 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault('complex_cache_decimals', 'd128', k, toDecimal128(42, 1))
    from default.decimals;

drop table default.decimals;
