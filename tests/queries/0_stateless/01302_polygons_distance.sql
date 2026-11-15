select polygonsDistanceCartesian([[[(0, 0),(0, 3),(1, 2.9),(2, 2.6),(2.6, 2),(2.9, 1),(3, 0),(0, 0)]]], [[[(1., 1.),(1., 4.),(4., 4.),(4., 1.),(1., 1.)]]]);
select polygonsDistanceCartesian([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(1., 1.),(1., 4.),(4., 4.),(4., 1.),(1., 1.)]]]);
select polygonsDistanceSpherical([[[(23.725750, 37.971536)]]], [[[(4.3826169, 50.8119483)]]]);

drop table if exists polygon_01302;
create table polygon_01302 (x Array(Array(Array(Tuple(Float64, Float64)))), y Array(Array(Array(Tuple(Float64, Float64))))) engine=Memory();
insert into polygon_01302 values ([[[(23.725750, 37.971536)]]], [[[(4.3826169, 50.8119483)]]]);
select polygonsDistanceSpherical(x, y) from polygon_01302;

drop table polygon_01302;
