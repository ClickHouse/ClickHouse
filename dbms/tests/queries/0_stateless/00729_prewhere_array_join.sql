SET send_logs_level = 'none';

drop table if exists t1;
create table t1 (id UInt64, val Array(String),nid UInt64, eDate Date)ENGINE = MergeTree(eDate, (id, eDate), 8192);

insert into t1 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (2,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (2,[],2,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (3,[],4,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (3,[],5,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (3,[],6,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (3,[],7,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (3,[],8,'2018-09-27');

select arrayJoin(val) as nameGroup6 from t1 prewhere notEmpty(toString(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }
select arrayJoin(val) as nameGroup6, countDistinct(nid) as rowids from t1 where notEmpty(toString(nameGroup6)) group by nameGroup6 order by nameGroup6;
select arrayJoin(val) as nameGroup6, countDistinct(nid) as rowids from t1 prewhere notEmpty(toString(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }

drop table t1;
create table t1 (id UInt64, val Array(String),nid UInt64, eDate Date) ENGINE = MergeTree(eDate, (id, eDate), 8192);

insert into t1 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (2,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1 (id,val,nid,eDate) values (2,[],2,'2018-09-27');

select arrayJoin(val) as nameGroup6 from t1 prewhere notEmpty(toString(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }
select arrayJoin(val) as nameGroup6, countDistinct(nid) as rowids from t1 where notEmpty(toString(nameGroup6)) group by nameGroup6 order by nameGroup6;
select arrayJoin(val) as nameGroup6, countDistinct(nid) as rowids from t1 prewhere notEmpty(toString(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }

drop table t1;
