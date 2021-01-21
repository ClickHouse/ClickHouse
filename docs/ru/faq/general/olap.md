---
title: What is OLAP?
toc_hidden: true
toc_priority: 100
---

# Что такое OLAP? {#what-is-olap}

[OLAP](https://ru.wikipedia.org/wiki/OLAP) переводится как интерактивная аналитическая обработка. Это широкий термин, который можно рассмотреть с двух сторон: технической и с точки зрения бизнеса. Но на самом верхнем уровне можно просто прочитать с конца:

Обработка
:   Обрабатываются некоторые иходные данные…

Аналитическая
:   …чтобы получить некоторые аналитические отчеты и новые идеи…

Интерактивная
:   …в реальном времени.

## OLAP с чтоки зрения бизнеса {#olap-from-the-business-perspective}

В последние годы, люди в бизнесе начали осознавать ценность данных. Компании, которые принимают решения вслепую, чаще всего отстают от конкурентов. Подход с учетом обработанных данных в успешных компаниях побуждает их собирать все данные, которые могут быть полезными в будущем для принятия бизнес-решений, а также подбирать механизмы, чтобы своевременно их анализировать. И вот тут приходит СУБД с OLAP. 

В понимании бизнеса, OLAP позволяет компаниями постоянно планировать, анализировать и собирать отчеты по операционной активности, такой как увеличение эффективности, уменьшение затрат и в итоге расширение доли своей значимости на рынке. It could be done either in an in-house system or outsourced to SaaS providers like web/mobile analytics services, CRM services, etc. OLAP is the technology behind many BI applications (Business Intelligence).

ClickHouse — это СУБД с OLAP, которая достаточно часто используется в роли бэкенда длдя таких SaaS-решений для анализа предметно-ориентированных данных. Тем не менее, некоторые компании все еще не слишком охотно деляться своими данными с провайдерами третьей стороны и 
ClickHouse is an OLAP database management system that is pretty often used as a backend for those SaaS solutions for analyzing domain-specific data. However, some businesses are still reluctant to share their data with third-party providers and an in-house data warehouse scenario is also viable.

## OLAP с точки зрения технической перспективы {#olap-from-the-technical-perspective}

Все СУБД можно классифицировать на две группы: OLAP (интерактивная **аналитическая** обработка) и OLTP (интерактивная обработка **транзакций**). Прошлые акценты на сборке отчетов, где каждый основывается на огромных объемах 

All database management systems could be classified into two groups: OLAP (Online **Analytical** Processing) and OLTP (Online **Transactional** Processing). Former focuses on building reports, each based on large volumes of historical data, but doing it not so frequently. While the latter usually handle a continuous stream of transactions, constantly modifying the current state of data.

In practice OLAP and OLTP are not categories, it’s more like a spectrum. Most real systems usually focus on one of them but provide some solutions or workarounds if the opposite kind of workload is also desired. This situation often forces businesses to operate multiple storage systems integrated, which might be not so big deal but having more systems make it more expensive to maintain. So the trend of recent years is HTAP (**Hybrid Transactional/Analytical Processing**) when both kinds of the workload are handled equally well by a single database management system.

Even if a DBMS started as a pure OLAP or pure OLTP, they are forced to move towards that HTAP direction to keep up with their competition. And ClickHouse is no exception, initially, it has been designed as [fast-as-possible OLAP system](../../faq/general/why-clickhouse-is-so-fast.md) and it still doesn’t have full-fledged transaction support, but some features like consistent read/writes and mutations for updating/deleting data had to be added.

The fundamental trade-off between OLAP and OLTP systems remains:

-   To build analytical reports efficiently it’s crucial to be able to read columns separately, thus most OLAP databases are [columnar](../../faq/general/columnar-database.md),
-   While storing columns separately increases costs of operations on rows, like append or in-place modification, proportionally to the number of columns (which can be huge if the systems try to collect all details of an event just in case). Thus, most OLTP systems store data arranged by rows.
