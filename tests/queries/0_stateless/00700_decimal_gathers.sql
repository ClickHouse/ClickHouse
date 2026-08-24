select if(1, [cast(materialize(2.0),'Decimal(9,3)')], [cast(materialize(1.0),'Decimal(9,3)')]);
select if(1, [cast(materialize(2.0),'Decimal(18,10)')], [cast(materialize(1.0),'Decimal(18,10)')]);
select if(1, [cast(materialize(2.0),'Decimal(38,18)')], [cast(materialize(1.0),'Decimal(38,18)')]);

select if(0, [cast(materialize(2.0),'Decimal(9,3)')], [cast(materialize(1.0),'Decimal(9,3)')]);
select if(0, [cast(materialize(2.0),'Decimal(18,10)')], [cast(materialize(1.0),'Decimal(18,10)')]);
select if(0, [cast(materialize(2.0),'Decimal(38,18)')], [cast(materialize(1.0),'Decimal(38,18)')]);

select '-';

select if(1, [cast(materialize(2.0),'Decimal(9,3)')], [cast(materialize(1.0),'Decimal(9,0)')]);
select if(0, [cast(materialize(2.0),'Decimal(18,10)')], [cast(materialize(1.0),'Decimal(18,0)')]);
select if(1, [cast(materialize(2.0),'Decimal(38,18)')], [cast(materialize(1.0),'Decimal(38,8)')]);

select if(0, [cast(materialize(2.0),'Decimal(9,0)')], [cast(materialize(1.0),'Decimal(9,3)')]);
select if(1, [cast(materialize(2.0),'Decimal(18,0)')], [cast(materialize(1.0),'Decimal(18,10)')]);
select if(0, [cast(materialize(2.0),'Decimal(38,0)')], [cast(materialize(1.0),'Decimal(38,18)')]);
