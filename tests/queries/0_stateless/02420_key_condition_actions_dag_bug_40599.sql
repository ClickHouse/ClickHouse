create table tba (event_id Int64, event_dt Int64) Engine =MergeTree order by event_id ;
insert into tba select number%500, 20220822 from numbers(1e6);

select count() from (
   SELECT event_dt FROM (
      select event_dt, 403 AS event_id from (
         select event_dt from tba as tba
         where event_id = 9 and ((tba.event_dt >= 20220822 and tba.event_dt <= 20220822))
      )
  ) tba WHERE tba.event_dt >= 20220822 and tba.event_dt <= 20220822 and event_id = 403 );
