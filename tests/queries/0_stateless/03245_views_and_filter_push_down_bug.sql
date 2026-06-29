select * from view(select id, toDateTime(date) as date from view(select 1 as id, '2024-05-02' as date)) where date='2024-05-02';
