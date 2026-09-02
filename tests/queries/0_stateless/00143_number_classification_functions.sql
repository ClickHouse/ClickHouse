select isFinite(0) = 1;
select isFinite(1) = 1;
select isFinite(materialize(0)) = 1;
select isFinite(materialize(1)) = 1;
select isFinite(1/0) = 0;
select isFinite(-1/0) = 0;
select isFinite(0/0) = 0;
select isFinite(inf) = 0;
select isFinite(-inf) = 0;
select isFinite(nan) = 0;

select isInfinite(0) = 0;
select isInfinite(1) = 0;
select isInfinite(materialize(0)) = 0;
select isInfinite(materialize(1)) = 0;
select isInfinite(1/0) = 1;
select isInfinite(-1/0) = 1;
select isInfinite(0/0) = 0;
select isInfinite(inf) = 1;
select isInfinite(-inf) = 1;
select isInfinite(nan) = 0;


select isNaN(0) = 0;
select isNaN(1) = 0;
select isNaN(materialize(0)) = 0;
select isNaN(materialize(1)) = 0;
select isNaN(1/0) = 0;
select isNaN(-1/0) = 0;
select isNaN(0/0) = 1;
select isNaN(inf) = 0;
select isNaN(-inf) = 0;
select isNaN(nan) = 1;
