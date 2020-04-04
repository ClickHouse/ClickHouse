---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 5
toc_title: "Caracter\xEDsticas de ClickHouse que pueden considerarse desventajas"
---

# Características de ClickHouse que pueden considerarse desventajas {#clickhouse-features-that-can-be-considered-disadvantages}

1.  No hay transacciones completas.
2.  Falta de capacidad para modificar o eliminar datos ya insertados con alta tasa y baja latencia. Hay eliminaciones y actualizaciones por lotes disponibles para limpiar o modificar datos, por ejemplo, para cumplir con [GDPR](https://gdpr-info.eu).
3.  El índice disperso hace que ClickHouse no sea tan adecuado para consultas de puntos que recuperan filas individuales por sus claves.

[Artículo Original](https://clickhouse.tech/docs/en/introduction/features_considered_disadvantages/) <!--hide-->
