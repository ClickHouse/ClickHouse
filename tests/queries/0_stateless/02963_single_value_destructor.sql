-- When we use SingleValueDataBaseMemoryBlock we must ensure we call the class destructor on destroy

Select argMax((number, number), (number, number)) FROM numbers(100000) format Null;
Select argMin((number, number), (number, number)) FROM numbers(100000) format Null;
Select anyHeavy((number, number)) FROM numbers(100000) format Null;
Select singleValueOrNull(number::Date32) FROM numbers(100000) format Null;
Select anyArgMax(number, (number, number)) FROM numbers(100000) format Null;
Select anyArgMin(number, (number, number)) FROM numbers(100000) format Null;
