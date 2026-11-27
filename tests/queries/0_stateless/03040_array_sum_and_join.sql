SET enable_analyzer=1;

select t.1 as cnt,
       t.2 as name,
       t.3 as percent
from (
      select arrayJoin(result) as t
      from (
            select [
                       (79, 'name1'),
                       (62, 'name2'),
                       (44, 'name3')
                       ]                                      as data,
                   arraySum(arrayMap(t -> t.1, data)) as total,
                   arrayMap(t ->
                                tuple(t.1, t.2,
                                      multiIf(total = 0, 0, t.1 > 0 and t.1 < 10, -1.0,
                                              (toFloat32(t.1) / toFloat32(total)) * 100)
                                    ),
                            data
                       )                                      as result
               )
         );

SELECT arrayMap(x -> arrayMap(x -> (x.1), [(1, 1), (2, 2)]), [(3, 3), (4, 4)]);

SELECT arrayMap(x -> (x.1, arrayMap(x -> (x.1), [(1, 1), (2, 2)])), [(3, 3), (4, 4)]);
