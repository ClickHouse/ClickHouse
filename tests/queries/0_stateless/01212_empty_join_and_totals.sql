select * from system.one t1
join system.one t2
on t1.dummy = t2.dummy
limit 0
FORMAT TabSeparated;

select * from system.one t1
join system.one t2
on t1.dummy = t2.dummy
where t2.dummy > 0
FORMAT TabSeparated;
