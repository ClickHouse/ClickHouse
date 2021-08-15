DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS db2;
DROP DATABASE IF EXISTS db3;
CREATE DATABASE db1 ENGINE=Ordinary;
CREATE DATABASE db2 ENGINE=Ordinary;
CREATE DATABASE db3 ENGINE=Ordinary;

select uuid from system.databases where name like 'db%' union distinct select uuid from system.databases where name like 'db%';
SELECT count() FROM　(SELECT * FROM system.databases WHERE name LIKE 'db%' UNION DISTINCT SELECT * FROM system.databases WHERE name LIKE 'db%');
select uuid from　(select * from system.databases where name like 'db%' union distinct select * from system.databases where name like 'db%');
