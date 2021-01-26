
set allow_experimental_map_type = 1;

-- Int type
select {1, 2, 3, 4};
select {1:2, 3:4};


-- String type
select {'1', '2', '3', '4'};
select {'1': '2', '3': '4'};

-- Int/String Mixed type
select {1, '1', 2, '2'};
select {1: '1', 2: '2'};
select {'1', 1, '2', 2};
select {'1': 1, '2': 2};
