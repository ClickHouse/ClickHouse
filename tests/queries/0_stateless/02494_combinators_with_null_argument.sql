-- { echoOn }

select sumIf(1, NULL);
select sumIf(NULL, 1);
select sumIf(NULL, NULL);
select countIf(1, NULL);
select countIf(NULL, 1);
select countIf(1, NULL);
select sumArray([NULL, NULL]);
select countArray([NULL, NULL]);

