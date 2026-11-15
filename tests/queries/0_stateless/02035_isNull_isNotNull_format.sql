-- { echo }
explain syntax select null is null;
explain syntax select null is not null;
explain syntax select isNull(null);
explain syntax select isNotNull(null);
explain syntax select isNotNull(1)+isNotNull(2) from remote('127.2', system.one);
select isNotNull(1)+isNotNull(2) from remote('127.2', system.one);
