-- { echoOn }
select arrayJoin([0.1, 1.1, 1.2, 1.3, 5.5, 5.6])::Decimal32(1) n, groupArray(n) over (order by n range 0.5 preceding) g;

-- Different precision in constant vs the column
select arrayJoin([0.1, 1.1, 1.2, 1.3, 5.5, 5.6])::Decimal32(1) n, groupArray(n) over (order by n range 0.5111::Decimal32(4) preceding) g;
select arrayJoin([0.1, 1.1, 1.2, 1.3, 5.5, 5.6])::Decimal32(3) n, groupArray(n) over (order by n range 0.5::Decimal32(1) preceding) g;

-- Can apply this to DateTime64 column as well by casting it to Decimal64(3)
select arrayJoin(['2022-01-01 01:01:01.111', '2022-01-01 01:01:01.222', '2022-01-01 01:01:03.333'])::DateTime64 t,
    groupArray(t) over (order by t::Decimal64(3) range 1.5 preceding) g;
