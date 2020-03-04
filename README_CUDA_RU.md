##Сборка
```
mkdir build_cuda
cd build_cuda
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_CUDA=1 -DCUDA_TOOLKIT_ROOT_DIR=/usr/local/cuda-9.1 -DNO_WERROR=1 ..
```
Здесь: -DUSE_CUDA наш параметер, т.е. компилим CUDA; -DCUDA_TOOLKIT_ROOT_DIR=/usr/local/cuda-9.1 - путь к cuda toolkit (если default 9.1, то можно не указывать); -DNO_WERROR=1 - это временно т.к. сил нет вычищать warnings.
make -j X clickhouse
Здесь X - количество потоков при сборке 

##Делаем генератор строк, на котором тестировали:
```
make generator
```

##Создаем таблицу в CH:
1. Запускаем сервер
   cd build_cuda/dbms/src/Server
   sudo ./clickhouse --server
2. Запускаем клиент
   ./clickhouse --clien
3. Создаем там таблицу с именем nnn:
   CREATE TABLE nnn (somedate Date, key String, value String) ENGINE =  MergeTree(somedate, (somedate), 8192);

4. Экспортируем данные в CH из string generator в таблицу nnn:
   ```
   cd build_cuda/dbms/src/Interpreters/Cuda/tests/StringGenerator
   ```
   В файле test_initialize.info устанавливаются нужные параметры таблицы.
   ```
   ./generator test_initialize.info
   ./generator test_read_file.info | ../../../../Server/clickhouse --client --query="INSERT INTO nnn FORMAT CSV"
   ```

##Работаем внутри CH client.

#режим агрегации на CPU:  SET use_cuda_aggregation=0
#режим агрегации на GPU:  SET use_cuda_aggregation=1

#Примеры запросов COUNT:
   ```
   SET use_cuda_aggregation=0
   SELECT key, COUNT(value) FROM nnn GROUP BY key LIMIT 10
   ```
10 rows in set. Elapsed: 3.895 sec. Processed 168.12 million rows, 7.92 GB (43.17 million rows/s., 2.03 GB/s.)
   ```
   SET use_cuda_aggregation=1
   SELECT key, COUNT(value) FROM nnn GROUP BY key LIMIT 10
   ```
10 rows in set. Elapsed: 1.556 sec. Processed 168.12 million rows, 7.92 GB (108.04 million rows/s., 5.09 GB/s.)

#примеры запросов uniqHLL12:
   ```
   SET use_cuda_aggregation=0
   SELECT key, uniqHLL12(value) FROM nnn GROUP BY key LIMIT 10
   ```
10 rows in set. Elapsed: 8.090 sec. Processed 168.12 million rows, 7.92 GB (20.78 million rows/s., 979.62 MB/s.

   ```
   SET use_cuda_aggregation=1
   SELECT key, uniqHLL12(value) FROM nnn GROUP BY key LIMIT 10
   ```
10 rows in set. Elapsed: 2.096 sec. Processed 168.12 million rows, 7.92 GB (80.20 million rows/s., 3.78 GB/s.) 

##Замечания
1. Для сравнения корректности cpu и gpu результатов с.м. пример в файле CUDA.md
2. Дальнейшие вопросы оптимизации надо обсуждать.


