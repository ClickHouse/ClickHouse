create table temp (
     x String EPHEMERAL,
     y String default x
)Engine=Memory;

insert into temp format JSONEachRow {"x":"xxx"};

select * from temp;
