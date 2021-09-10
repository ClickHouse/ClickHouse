insert into table function file('test/parquet_use_virtual/data.parquet', 'Parquet', 's String') values ('value');
select splitByString('test/', _path)[2] as relative, _file, s from file('test/parquet_use_virtual/data.parquet', 'Parquet', 's String');
select _file, splitByString('test/', _path)[2] as relative, s from file('test/parquet_use_virtual/data.parquet', 'Parquet', 's String');
