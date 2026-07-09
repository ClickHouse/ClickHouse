---
alias: []
description: 'Документация по формату JSONStringsEachRow'
input_format: true
keywords: ['JSONStringsEachRow']
output_format: true
slug: /interfaces/formats/JSONStringsEachRow
title: 'JSONStringsEachRow'
doc_type: 'reference'
---

| Ввод | Вывод | Алиас |
| ---- | ----- | ----- |
| ✔    | ✔     |       |

<div id="description">
  ## Описание
</div>

Отличается от [`JSONEachRow`](./JSONEachRow.md) только тем, что поля данных выводятся как строки, а не как типизированные значения JSON.

<div id="example-usage">
  ## Пример использования
</div>

<div id="inserting-data">
  ### Вставка данных
</div>

Используйте JSON‑файл со следующими данными под названием `football.json`:

```json
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
{"date":"2022-04-30","season":"2021","home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-02","season":"2021","home_team":"Port Vale","away_team":"Newport County","home_team_goals":"1","away_team_goals":"2"}
{"date":"2022-05-02","season":"2021","home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Barrow","away_team":"Northampton Town","home_team_goals":"1","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":"7","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Exeter City","away_team":"Port Vale","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Newport County","away_team":"Rochdale","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":"3","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":"4","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Walsall","away_team":"Swindon Town","home_team_goals":"0","away_team_goals":"3"}   
```

Вставьте данные:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONStringsEachRow;
```

<div id="reading-data">
  ### Чтение данных
</div>

Для чтения данных используйте формат `JSONStringsEachRow`:

```sql
SELECT *
FROM football
FORMAT JSONStringsEachRow
```

Вывод будет в формате JSON:

```json
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
{"date":"2022-04-30","season":"2021","home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-02","season":"2021","home_team":"Port Vale","away_team":"Newport County","home_team_goals":"1","away_team_goals":"2"}
{"date":"2022-05-02","season":"2021","home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Barrow","away_team":"Northampton Town","home_team_goals":"1","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":"7","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Exeter City","away_team":"Port Vale","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Newport County","away_team":"Rochdale","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":"3","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":"4","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Walsall","away_team":"Swindon Town","home_team_goals":"0","away_team_goals":"3"}   
```

<div id="format-settings">
  ## Настройки формата
</div>
