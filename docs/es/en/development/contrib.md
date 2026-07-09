---
description: 'Página que describe el uso de bibliotecas de terceros en ClickHouse y cómo agregar y mantener
  bibliotecas de terceros.'
sidebar_label: 'Bibliotecas de terceros'
sidebar_position: 60
slug: /development/contrib
title: 'Bibliotecas de terceros'
doc_type: 'reference'
---

ClickHouse utiliza bibliotecas de terceros para distintos fines; por ejemplo, para conectarse a otras bases de datos, para decodificar/codificar datos durante la carga o el guardado desde o hacia disco, o para implementar determinadas funciones SQL especializadas.
Para no depender de las bibliotecas disponibles en el sistema de destino, cada biblioteca de terceros se importa como un Git submódulo en el árbol de código fuente de ClickHouse, y se compila y enlaza con ClickHouse.
Se puede obtener una lista de bibliotecas de terceros y sus licencias mediante la siguiente consulta:

```sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

Tenga en cuenta que las bibliotecas enumeradas son las que se encuentran en el directorio `contrib/` del repositorio de ClickHouse.
En función de las opciones de compilación, es posible que algunas bibliotecas no se hayan compilado y, por lo tanto, que su funcionalidad no esté disponible en tiempo de ejecución.

[Ejemplo](https://sql.clickhouse.com?query_id=478GCPU7LRTSZJBNY3EJT3)

<div id="adding-and-maintaining-third-party-libraries">
  ## Añadir y mantener bibliotecas de terceros
</div>

Cada biblioteca de terceros debe residir en un directorio dedicado dentro del directorio `contrib/` del repositorio de ClickHouse.
Evite volcar copias de código externo en el directorio de la biblioteca.
En su lugar, cree un submódulo de Git para traer código de terceros desde un repositorio upstream externo.

Todos los submódulos usados por ClickHouse se enumeran en el archivo `.gitmodule`.

* Si la biblioteca puede usarse tal cual (el caso predeterminado), puede hacer referencia directamente al repositorio upstream.
* Si la biblioteca necesita parches, cree un fork del repositorio upstream en la [organización de ClickHouse en GitHub](https://github.com/ClickHouse).

En este último caso, el objetivo es aislar los parches personalizados lo máximo posible de los commits upstream.
Para ello, cree una rama con el prefijo `ClickHouse/` a partir de la rama o la tag que quiera integrar, por ejemplo `ClickHouse/2024_2` (para la rama `2024_2`) o `ClickHouse/release/vX.Y.Z` (para la tag `release/vX.Y.Z`).
Evite seguir las ramas de desarrollo upstream `master`/ `main` / `dev` (es decir, ramas con prefijo `ClickHouse/master` / `ClickHouse/main` / `ClickHouse/dev` en el repositorio con fork).
Estas ramas son objetivos móviles que dificultan un versionado adecuado.
Las &quot;ramas con prefijo&quot; garantizan que los pulls desde el repositorio upstream hacia el fork dejen intactas las ramas personalizadas `ClickHouse/`.
Los submódulos de `contrib/` solo deben seguir ramas `ClickHouse/` de repositorios de terceros con fork.

Los parches solo se aplican sobre ramas `ClickHouse/` de bibliotecas externas.

Hay dos formas de hacerlo:

* quiere crear una nueva corrección sobre una rama con prefijo `ClickHouse/` en el repositorio con fork, por ejemplo una corrección de sanitizador. En ese caso, publique la corrección como una rama con prefijo `ClickHouse/`, por ejemplo `ClickHouse/fix-sanitizer-disaster`. Después, cree una PR desde la nueva rama hacia la rama de seguimiento personalizada, por ejemplo `ClickHouse/2024_2 <-- ClickHouse/fix-sanitizer-disaster`, y haga merge de la PR.
* actualiza el submódulo y necesita volver a aplicar parches anteriores. En este caso, volver a crear PR antiguas es excesivo. En su lugar, simplemente haga cherry-pick de commits anteriores en la nueva rama `ClickHouse/` (correspondiente a la nueva versión). No dude en hacer squash de los commits de PR que tenían varios commits. En el mejor de los casos, ya habremos aportado los parches personalizados a upstream y podremos omitirlos en la nueva versión.

Una vez que se haya actualizado el submódulo, actualice el submódulo en ClickHouse para que apunte al nuevo hash del fork.

Cree parches para bibliotecas de terceros pensando en el repositorio oficial y considere aportar el parche de vuelta al repositorio upstream.
Esto garantiza que otros también se beneficien del parche y que no suponga una carga de mantenimiento para el equipo de ClickHouse.