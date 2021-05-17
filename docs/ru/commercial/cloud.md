---
toc_priority: 1
toc_title: "Поставщики облачных услуг ClickHouse"
---

# Поставщики облачных услуг ClickHouse {#clickhouse-cloud-service-providers}

!!! info "Инфо"
    Если вы запустили публичный облачный сервис с управляемым ClickHouse, не стесняйтесь [открыть pull request](https://github.com/ClickHouse/ClickHouse/edit/master/docs/en/commercial/cloud.md) c добавлением его в последующий список.

## Yandex Cloud {#yandex-cloud}

[Yandex Managed Service for ClickHouse](https://cloud.yandex.ru/services/managed-clickhouse?utm_source=referrals&utm_medium=clickhouseofficialsite&utm_campaign=link3) предоставляет следующие ключевые возможности:

-   полностью управляемый сервис ZooKeeper для [репликации ClickHouse](../engines/table-engines/mergetree-family/replication.md)
-   выбор типа хранилища
-   реплики в разных зонах доступности
-   шифрование и изоляция
-   автоматизированное техническое обслуживание

## Altinity.Cloud {#altinity.cloud}

[Altinity.Cloud](https://altinity.com/cloud-database/) — это полностью управляемый ClickHouse-as-a-Service для публичного облака Amazon.

-   быстрое развертывание кластеров ClickHouse на ресурсах Amazon.
-   легкое горизонтальное масштабирование также, как и вертикальное масштабирование узлов.
-   изолированные виртуальные сети для каждого клиента с общедоступным эндпоинтом или пирингом VPC.
-   настраиваемые типы и объемы хранилищ
-   cross-az масштабирование для повышения производительности и обеспечения высокой доступности
-   встроенный мониторинг и редактор SQL-запросов

## Alibaba Cloud {#alibaba-cloud}

Управляемый облачный сервис Alibaba для ClickHouse: [китайская площадка](https://www.aliyun.com/product/clickhouse), будет доступен на международной площадке в мае 2021 года. Сервис предоставляет следующие возможности:

-   надежный сервер для облачного хранилища на основе распределенной системы [Alibaba Cloud Apsara](https://www.alibabacloud.com/product/apsara-stack);
-   расширяемая по запросу емкость, без переноса данных вручную;
-   поддержка одноузловой и многоузловой архитектуры, архитектуры с одной или несколькими репликами, а также многоуровневого хранения cold и hot data;
-   поддержка прав доступа, one-key восстановления, многоуровневая защита сети, шифрование облачного диска;
-   полная интеграция с облачными системами логирования, базами данных и инструментами обработки данных;
-   встроенная платформа для мониторинга и управления базами данных;
-   техническая поддержка от экспертов по работе с базами данных.