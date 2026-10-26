SET enable_analyzer = 1;

select so,
       r
from
    (select  [('y',0),('n',1)] as cg,
             if( arrayMap( x -> x.1, cg ) != ['y', 'n'], 'y', 'n')  as so,
             arrayFilter( x -> x.1 = so , cg) as r
    );

select
       r
from
    (select  [('y',0),('n',1)] as cg,
             if( arrayMap( x -> x.1, cg ) != ['y', 'n'], 'y', 'n')  as so,
             arrayFilter( x -> x.1 = so , cg) as r
    );
