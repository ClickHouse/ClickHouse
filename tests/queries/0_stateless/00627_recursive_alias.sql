select x from (select dummy as x, dummy + 1 as dummy order by identity(x)) format Null;
