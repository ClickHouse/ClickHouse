select round(polygonsDistanceCartesian([[[(0, 0),(0, 3),(1, 2.9),(2, 2.6),(2.6, 2),(2.9, 1),(3, 0),(0, 0)]]], [[[(1., 1.),(1., 4.),(4., 4.),(4., 1.),(1., 1.)]]]), 6);
select round(polygonsDistanceCartesian([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(1., 1.),(1., 4.),(4., 4.),(4., 1.),(1., 1.)]]]), 6);
select round(polygonsDistanceSpherical([[[(23.725750, 37.971536)]]], [[[(4.3826169, 50.8119483)]]]), 6);

drop table if exists polygon_01302;
create table polygon_01302 (x Array(Array(Array(Tuple(Float64, Float64)))), y Array(Array(Array(Tuple(Float64, Float64))))) engine=Memory();
insert into polygon_01302 values ([[[(23.725750, 37.971536)]]], [[[(4.3826169, 50.8119483)]]]);
select round(polygonsDistanceSpherical(x, y), 6) from polygon_01302;

drop table polygon_01302;
