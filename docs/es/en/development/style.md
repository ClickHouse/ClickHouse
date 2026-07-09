---
description: 'Guía de estilo de código para el desarrollo de ClickHouse en C++'
sidebar_label: 'Guía de estilo de C++'
sidebar_position: 70
slug: /development/style
title: 'Guía de estilo de C++'
doc_type: 'guide'
---

<div id="general-recommendations">
  ## Recomendaciones generales
</div>

Lo siguiente son recomendaciones, no requisitos.
Si está editando código, tiene sentido seguir el formato del código existente.
El estilo de código es necesario para mantener la consistencia. La consistencia facilita la lectura del código y también su búsqueda.
Muchas de las reglas no tienen una justificación lógica; vienen dictadas por las prácticas establecidas.

<div id="formatting">
  ## Formato
</div>

**1.** La mayor parte del formateo se realiza automáticamente con `clang-format`.

**2.** La sangría es de 4 espacios. Configure su entorno de desarrollo para que la tecla de tabulación inserte cuatro espacios.

**3.** Las llaves de apertura y cierre deben ir en una línea separada.

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** Si el cuerpo completo de la función es un único `statement`, puede colocarse en una sola línea. Coloque espacios alrededor de las llaves (además del espacio al final de la línea).

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** Para funciones. No pongas espacios alrededor de los paréntesis.

```cpp
void reinsert(const Value & x)
```

```cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** En las expresiones `if`, `for`, `while` y otras similares, se inserta un espacio antes del paréntesis de apertura (a diferencia de las llamadas a funciones).

```cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** Añade espacios alrededor de los operadores binarios (`+`, `-`, `*`, `/`, `%`, ...) y el operador ternario `?:`.

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** Si se introduce un salto de línea, coloque el operador en una nueva línea y aumente la sangría delante de él.

```cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** Puede usar espacios para alinear el contenido dentro de una línea, si así lo desea.

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** No uses espacios alrededor de los operadores `.`, `->`.

Si es necesario, el operador puede pasarse a la línea siguiente. En ese caso, la sangría delante de él aumenta.

**11.** No utilice un espacio para separar los operadores unarios (`--`, `++`, `*`, `&`, ...) del argumento.

**12.** Coloca un espacio después de una coma, pero no antes. La misma regla se aplica para un punto y coma dentro de una expresión `for`.

**13.** No utilice espacios para separar el operador `[]`.

**14.** En una expresión `template <...>`, utiliza un espacio entre `template` y `<`; sin espacios después de `<` ni antes de `>`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** En clases y estructuras, escribe `public`, `private` y `protected` al mismo nivel que `class/struct`, e indenta el resto del código.

```cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** Si el mismo `namespace` se usa en todo el archivo y no hay nada más relevante, no es necesaria una sangría dentro de `namespace`.

**17.** Si el bloque de un `if`, `for`, `while` u otra expresión consta de un único `statement`, las llaves son opcionales. En su lugar, coloque el `statement` en una línea aparte. Esta regla también aplica para `if`, `for`, `while` anidados, ...

Pero si la `statement` interna contiene llaves o `else`, el bloque externo debe escribirse entre llaves.

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** No debe haber espacios al final de las líneas.

**19.** Los archivos fuente están codificados en UTF-8.

**20.** Los caracteres no ASCII pueden usarse en literales de cadena.

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** No escriba varias expresiones en una sola línea.

**22.** Agrupe las secciones de código dentro de las funciones y sepárelas con no más de una línea en blanco.

**23.** Separe las funciones, las clases, etc., con una o dos líneas en blanco.

**24.** `A const` (cuando se refiere a un valor) debe escribirse antes del nombre del tipo.

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** Al declarar un puntero o una referencia, los símbolos `*` y `&` deben ir separados por espacios a ambos lados.

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Al usar tipos de plantilla, asígneles un alias con la palabra clave `using` (excepto en los casos más sencillos).

En otras palabras, los parámetros de plantilla se especifican solo en `using` y no se repiten en el código.

`using` puede declararse localmente, por ejemplo, dentro de una función.

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** No declare varias variables de distintos tipos en una misma declaración.

```cpp
//incorrect
int x, *y;
```

**28.** No use conversiones de tipo de estilo C.

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** En clases y structs, agrupe por separado los miembros y las funciones dentro de cada ámbito de visibilidad.

**30.** En clases y structs pequeños, no es necesario separar la declaración del método de su implementación.

Lo mismo se aplica a los métodos pequeños en cualquier clase o struct.

En las clases y structs de plantilla, no separe las declaraciones de los métodos de su implementación (porque, de lo contrario, deben definirse en la misma unidad de traducción).

**31.** Puede partir las líneas a 140 caracteres, en lugar de 80.

**32.** Utilice siempre los operadores prefijo de incremento/decremento si no se requiere el posfijo.

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

<div id="comments">
  ## Comentarios
</div>

**1.** Asegúrate de añadir comentarios en todos los fragmentos del código que no sean triviales.

Esto es muy importante. Escribir el comentario puede ayudarte a darte cuenta de que el código no es necesario o de que está mal diseñado.

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Los comentarios pueden ser tan detallados como sea necesario.

**3.** Coloca los comentarios antes del código al que se refieren. En casos excepcionales, pueden ir después del código, en la misma línea.

```cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// Here could be written the description on how query was executed
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// Up to which stage process the SELECT query
    )
```

**4.** Los comentarios deben escribirse únicamente en inglés.

**5.** Si escribes una biblioteca, incluye comentarios detallados que la expliquen en el archivo de cabecera principal.

**6.** No añadas comentarios que no aporten información adicional. En particular, no dejes comentarios vacíos como este:

```cpp
/*
* Procedure Name:
* Original procedure name:
* Author:
* Date of creation:
* Dates of modification:
* Modification authors:
* Original file name:
* Purpose:
* Intent:
* Designation:
* Classes used:
* Constants:
* Local variables:
* Parameters:
* Date of creation:
* Purpose:
*/
```

El ejemplo está tomado de http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/.

**7.** No escriba comentarios basura (autor, fecha de creación...) al principio de cada archivo.

**8.** Los comentarios de una sola línea comienzan con tres barras: `///` y los comentarios multilínea comienzan con `/**`. Estos comentarios se consideran &quot;documentación&quot;.

Nota: Puede usar Doxygen para generar documentación a partir de estos comentarios. Pero, en general, no se usa Doxygen porque es más cómodo navegar por el código en el IDE.

**9.** Los comentarios multilínea no deben tener líneas vacías al principio ni al final (excepto la línea que cierra un comentario multilínea).

**10.** Para comentar código, use comentarios básicos, no comentarios de &quot;documentación&quot;.

**11.** Elimine las partes comentadas del código antes de hacer commit.

**12.** No use palabrotas en los comentarios ni en el código.

**13.** No use letras mayúsculas. No use una puntuación excesiva.

```cpp
/// WHAT THE FAIL???
```

**14.** No use comentarios como delimitadores.

```cpp
///******************************************************
```

**15.** No abras debates en los comentarios.

```cpp
/// Why did you do this stuff?
```

**16.** No hace falta escribir un comentario al final de un bloque para explicar de qué trata.

```cpp
/// for
```

<div id="names">
  ## Nombres
</div>

**1.** Usa minúsculas y guiones bajos en los nombres de las variables y los miembros de class.

```cpp
size_t max_block_size;
```

**2.** Para los nombres de las funciones (métodos), use `camelCase`, empezando con una letra minúscula.

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** Para los nombres de las class (structs), use CamelCase y comience con una letra mayúscula. No se usan prefijos distintos de I para las interfaces.

```cpp
class StorageMemory : public IStorage
```

**4.** `using` se nombran igual que las class.

**5.** Nombres de los parámetros de tipo de plantilla: en casos sencillos, use `T`; `T`, `U`; `T1`, `T2`.

En casos más complejos, siga las reglas para los nombres de class o añada el prefijo `T`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Los nombres de los argumentos constantes de la plantilla: o bien deben seguir las reglas de nomenclatura de las variables, o bien usar `N` en los casos sencillos.

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** A las class abstractas (interfaces) puede añadirles el prefijo `I`.

```cpp
class IProcessor
```

**8.** Si usas una variable en un ámbito local, puedes usar el nombre corto.

En todos los demás casos, usa un nombre que describa su significado.

```cpp
bool info_successfully_loaded = false;
```

**9.** Los nombres de los `define` y de las constantes globales se escriben en ALL&#95;CAPS con guiones bajos.

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** Los nombres de los archivos deben seguir el mismo estilo que su contenido.

Si un archivo contiene una sola class, asígnale al archivo el mismo nombre que la class (CamelCase).

Si el archivo contiene una sola función, asígnale al archivo el mismo nombre que la función (camelCase).

**11.** Si el nombre contiene una abreviatura, entonces:

* En los nombres de variables, la abreviatura debe ir en minúsculas: `mysql_connection` (no `mySQL_connection`).
* En los nombres de class y funciones, mantén las mayúsculas de la abreviatura: `MySQLConnection` (no `MySqlConnection`).

**12.** Los argumentos del constructor que se usan únicamente para inicializar los miembros de la class deben tener el mismo nombre que esos miembros, pero con un guion bajo al final.

```cpp
FileQueueProcessor(
    const std::string & path_,
    const std::string & prefix_,
    std::shared_ptr<FileHandler> handler_)
    : path(path_),
    prefix(prefix_),
    handler(handler_),
    log(&Logger::get("FileQueueProcessor"))
{
}
```

El sufijo de guion bajo puede omitirse si el argumento no se utiliza en el cuerpo del constructor.

**13.** No hay diferencia entre los nombres de las variables locales y los de los miembros de la class (no se requieren prefijos).

```cpp
timer (not m_timer)
```

**14.** Para las constantes de un `enum`, usa CamelCase con inicial mayúscula. ALL&#95;CAPS también es aceptable. Si el `enum` no es local, usa un `enum class`.

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** Todos los nombres deben estar en inglés. No se permite la transliteración de palabras en hebreo.

no T&#95;PAAMAYIM&#95;NEKUDOTAYIM

**16.** Se aceptan abreviaturas si son ampliamente conocidas (cuando puedes encontrar fácilmente su significado en Wikipedia o en un motor de búsqueda).

`AST`, `SQL`.

No `NVDH` (unas letras aleatorias)

Se aceptan palabras incompletas si la versión abreviada es de uso común.

También puedes usar una abreviatura si el nombre completo aparece junto a ella en los comentarios.

**17.** Los nombres de archivo con código fuente de C++ deben tener la extensión `.cpp`. Los header files deben tener la extensión `.h`.

<div id="how-to-write-code">
  ## Cómo escribir código
</div>

**1.** Gestión de memoria.

La liberación manual de memoria (`delete`) solo puede usarse en el código de biblioteca.

En el código de biblioteca, el operador `delete` solo puede usarse en los destructores.

En el código de aplicación, la memoria debe liberarla el objeto que la posee.

Ejemplos:

* La forma más sencilla es colocar un objeto en la pila o hacerlo miembro de otra clase.
* Para una gran cantidad de objetos pequeños, use contenedores.
* Para la liberación automática de una pequeña cantidad de objetos que residen en el heap, use `shared_ptr/unique_ptr`.

**2.** Gestión de recursos.

Use `RAII` y vea lo anterior.

**3.** Manejo de errores.

Use excepciones. En la mayoría de los casos, solo necesita lanzar una excepción y no es necesario capturarla (gracias a `RAII`).

En las aplicaciones de procesamiento de datos offline, a menudo es aceptable no capturar excepciones.

En los servidores que manejan solicitudes de usuarios, normalmente basta con capturar las excepciones en el nivel superior del manejador de la conexión.

En las funciones de hilo, debe capturar y conservar todas las excepciones para volver a lanzarlas en el hilo principal después de `join`.

```cpp
/// If there weren't any calculations yet, calculate the first block synchronously
if (!started)
{
    calculate();
    started = true;
}
else /// If calculations are already in progress, wait for the result
    pool.wait();

if (exception)
    exception->rethrow();
```

Nunca ocultes las excepciones sin gestionarlas. Nunca envíes todas las excepciones al log sin más.

```cpp
//Not correct
catch (...) {}
```

Si necesitas ignorar algunas excepciones, hazlo solo con excepciones específicas y vuelve a lanzar las demás.

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

Al usar funciones con códigos de respuesta o `errno`, compruebe siempre el resultado y lance una excepción en caso de error.

```cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

Puede usar assert para comprobar invariantes en el código.

**4.** Tipos de excepción.

No es necesario usar una jerarquía de excepciones compleja en el código de la aplicación. El texto de la excepción debe ser comprensible para un administrador de sistemas.

**5.** Lanzar excepciones desde destructores.

No se recomienda, pero está permitido.

Use las siguientes opciones:

* Cree una función (`done()` o `finalize()`) que realice de antemano todo el trabajo que pueda provocar una excepción. Si se llamó a esa función, después no debería producirse ninguna excepción en el destructor.
* Las tareas demasiado complejas (como enviar mensajes por la red) pueden colocarse en un método independiente que el usuario de la clase deberá llamar antes de la destrucción.
* Si se produce una excepción en el destructor, es mejor registrarla que ocultarla (si el logger está disponible).
* En aplicaciones simples, es aceptable confiar en `std::terminate` (para los casos de `noexcept` predeterminado en C++11) para manejar excepciones.

**6.** Bloques de código anónimos.

Puede crear un bloque de código independiente dentro de una sola función para hacer que determinadas variables sean locales, de modo que los destructores se llamen al salir del bloque.

```cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multihilo.

En programas de procesamiento de datos offline:

* Intenta obtener el mejor rendimiento posible en un solo núcleo de CPU. Después, si es necesario, puedes paralelizar tu código.

En aplicaciones de servidor:

* Usa el grupo de hilos para procesar solicitudes. Hasta ahora, no hemos tenido ninguna tarea que requiriera cambio de contexto en espacio de usuario.

No se usa fork para la paralelización.

**8.** Sincronización de hilos.

A menudo es posible hacer que distintos hilos usen distintas celdas de memoria (mejor aún: distintas líneas de caché) y no usar ninguna sincronización entre hilos (excepto `joinAll`).

Si se requiere sincronización, en la mayoría de los casos basta con usar un mutex con `lock_guard`.

En otros casos, usa primitivas de sincronización del sistema. No uses espera activa.

Las operaciones atómicas deben usarse solo en los casos más simples.

No intentes implementar estructuras de datos sin bloqueo a menos que sea tu principal área de especialización.

**9.** Punteros frente a referencias.

En la mayoría de los casos, prefiere las referencias.

**10.** `const`.

Usa referencias constantes, punteros a constantes, `const_iterator` y métodos `const`.

Considera `const` como la opción predeterminada y usa no-`const` solo cuando sea necesario.

Al pasar variables por valor, usar `const` normalmente no tiene sentido.

**11.** unsigned.

Usa `unsigned` si es necesario.

**12.** Tipos numéricos.

Usa los tipos `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32` y `Int64`, así como `size_t`, `ssize_t` y `ptrdiff_t`.

No uses estos tipos para números: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** Paso de argumentos.

Pasa los valores complejos por valor si se van a mover y usa `std::move`; pásalos por referencia si quieres actualizar el valor en un bucle.

Si una función asume la propiedad de un objeto creado en el heap, haz que el tipo del argumento sea `shared_ptr` o `unique_ptr`.

**14.** Valores de retorno.

En la mayoría de los casos, simplemente usa `return`. No escribas `return std::move(res)`.

Si la función asigna un objeto en el heap y lo devuelve, usa `shared_ptr` o `unique_ptr`.

En casos excepcionales (como actualizar un valor en un bucle), puede que necesites devolver el valor mediante un argumento. En ese caso, el argumento debe ser una referencia.

```cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** `namespace`.

No es necesario usar un `namespace` independiente para el código de aplicación.

Las bibliotecas pequeñas tampoco lo necesitan.

En bibliotecas medianas o grandes, ponlo todo dentro de un `namespace`.

En el archivo `.h` de la biblioteca, puedes usar `namespace detail` para ocultar detalles de implementación que el código de aplicación no necesita.

En un archivo `.cpp`, puedes usar un `namespace` anónimo o `static` para ocultar símbolos.

Además, se puede usar un `namespace` para un `enum` a fin de evitar que los nombres correspondientes queden en un `namespace` externo (aunque es mejor usar un `enum class`).

**16.** Inicialización diferida.

Si se requieren argumentos para la inicialización, normalmente no deberías escribir un constructor por defecto.

Si más adelante necesitas retrasar la inicialización, puedes añadir un constructor por defecto que cree un objeto no válido. O, si son pocos objetos, puedes usar `shared_ptr/unique_ptr`.

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** Funciones virtuales.

Si la class no está pensada para uso polimórfico, no es necesario que las funciones sean virtuales. Esto también se aplica al destructor.

**18.** Codificaciones.

Use UTF-8 en todas partes. Use `std::string` y `char *`. No use `std::wstring` ni `wchar_t`.

**19.** Logging.

Vea ejemplos en todo el código.

Antes de hacer commit, elimine todo el logging irrelevante y de depuración, así como cualquier otro tipo de salida de depuración.

Debe evitarse el logging dentro de bucles, incluso en el nivel Trace.

Los logs deben ser legibles en cualquier nivel de logging.

El logging debe usarse, en general, solo en el código de la aplicación.

Los mensajes de log deben escribirse en inglés.

Preferiblemente, el log debe ser comprensible para el administrador del sistema.

No use lenguaje ofensivo en el log.

Use codificación UTF-8 en el log. En casos excepcionales, puede usar caracteres no ASCII en el log.

**20.** Entrada/salida.

No use `iostreams` en bucles internos críticos para el rendimiento de la aplicación (y nunca use `stringstream`).

Use en su lugar la biblioteca `DB/IO`.

**21.** Fecha y hora.

Vea la biblioteca `DateLUT`.

**22.** include.

Use siempre `#pragma once` en lugar de guardas de inclusión.

**23.** using.

No se usa `using namespace`. Puede usar `using` para algo específico. Pero hágalo local, dentro de una class o función.

**24.** No use `trailing return type` para las funciones, salvo que sea necesario.

```cpp
auto f() -> void
```

**25.** Declaración e inicialización de variables.

```cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** Para las funciones virtuales, escriba `virtual` en la class base, pero `override` en lugar de `virtual` en las class derivadas.

<div id="unused-features-of-c">
  ## Características de C++ no utilizadas
</div>

**1.** No se usa la herencia virtual.

**2.** Construcciones que tienen azúcar sintáctico útil en el C++ moderno, p. ej.

```cpp
// Traditional way without syntactic sugar
template <typename G, typename = std::enable_if_t<std::is_same<G, F>::value, void>> // SFINAE via std::enable_if, usage of ::value
std::pair<int, int> func(const E<G> & e) // explicitly specified return type
{
    if (elements.count(e)) // .count() membership test
    {
        // ...
    }

    elements.erase(
        std::remove_if(
            elements.begin(), elements.end(),
            [&](const auto x){
                return x == 1;
            }),
        elements.end()); // remove-erase idiom

    return std::make_pair(1, 2); // create pair via make_pair()
}

// With syntactic sugar (C++14/17/20)
template <typename G>
requires std::same_v<G, F> // SFINAE via C++20 concept, usage of C++14 template alias
auto func(const E<G> & e) // auto return type (C++14)
{
    if (elements.contains(e)) // C++20 .contains membership test
    {
        // ...
    }

    elements.erase_if(
        elements,
        [&](const auto x){
            return x == 1;
        }); // C++20 std::erase_if

    return {1, 2}; // or: return std::pair(1, 2); // create pair via initialization list or value initialization (C++17)
}
```

<div id="platform">
  ## Plataforma
</div>

**1.** Escribimos código para una plataforma específica.

Pero, a igualdad de condiciones, se prefiere el código multiplataforma o portable.

**2.** Lenguaje: C++20 (consulte la lista de [funcionalidades disponibles de C++20](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features)).

**3.** Compilador: `clang`. En el momento de escribir esto (marzo de 2025), el código se compila con clang versión &gt;= 19.

Se utiliza la biblioteca estándar (`libc++`).

**4.** SO: Ubuntu Linux, versión Precise o posterior.

**5.** El código está escrito para la arquitectura de CPU x86&#95;64.

El conjunto de instrucciones de la CPU es el conjunto mínimo admitido entre nuestros servidores. Actualmente, es SSE 4.2.

**6.** Use los indicadores de compilación `-Wall -Wextra -Werror -Weverything` con unas pocas excepciones.

**7.** Use enlazado estático con todas las bibliotecas, excepto aquellas que son difíciles de enlazar de forma estática (consulte la salida del comando `ldd`).

**8.** El código se desarrolla y se depura en modo release.

<div id="tools">
  ## Herramientas
</div>

**1.** KDevelop es un buen IDE.

**2.** Para depurar, use `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...` o `tcmalloc_minimal_debug`.

**3.** Para el perfilado, use `Linux Perf`, `valgrind` (`callgrind`) o `strace -cf`.

**4.** El código fuente está en Git.

**5.** La compilación se hace con `CMake`.

**6.** Los programas se distribuyen en paquetes `deb`.

**7.** Los commits a master no deben romper la compilación.

Aunque solo algunas revisiones se consideran utilizables.

**8.** Haga commits con la mayor frecuencia posible, incluso si el código solo está parcialmente listo.

Use ramas para ello.

Si su código en la rama `master` todavía no compila, exclúyalo de la compilación antes del `push`. Tendrá que terminarlo o eliminarlo en unos días.

**9.** Para cambios no triviales, use ramas y publíquelas en el servidor.

**10.** El código no utilizado se elimina del repositorio.

<div id="libraries">
  ## Bibliotecas
</div>

**1.** Se usa la biblioteca estándar de C++20 (se permiten extensiones experimentales), así como los frameworks `boost` y `Poco`.

**2.** No se permite usar bibliotecas de paquetes del sistema operativo. Tampoco se permite usar bibliotecas preinstaladas. Todas las bibliotecas deben incluirse en forma de código fuente en el directorio `contrib` y compilarse junto con ClickHouse. Consulte [Directrices para agregar nuevas bibliotecas de terceros](/es/development/contrib#adding-and-maintaining-third-party-libraries) para obtener más detalles.

**3.** Siempre se da preferencia a las bibliotecas que ya se estén usando.

<div id="general-recommendations">
  ## Recomendaciones generales
</div>

**1.** Escriba la menor cantidad de código posible.

**2.** Pruebe la solución más sencilla.

**3.** No escriba código hasta que sepa cómo va a funcionar y cómo funcionará el bucle interno.

**4.** En los casos más simples, use `using` en lugar de clases o structs.

**5.** Si es posible, no escriba constructores de copia, operadores de asignación, destructores (salvo uno virtual, si la class contiene al menos una función virtual), constructores de movimiento ni operadores de asignación por movimiento. En otras palabras, las funciones generadas por el compilador deben funcionar correctamente. Puede usar `default`.

**6.** Se recomienda simplificar el código. Reduzca su tamaño siempre que sea posible.

<div id="additional-recommendations">
  ## Recomendaciones adicionales
</div>

**1.** No se recomienda especificar explícitamente `std::` para los tipos de `stddef.h`

En otras palabras, recomendamos escribir `size_t` en lugar de `std::size_t`, porque es más corto.

Es aceptable añadir `std::`.

**2.** No se recomienda especificar explícitamente `std::` para las funciones de la biblioteca estándar de C

En otras palabras, escriba `memcpy` en lugar de `std::memcpy`.

La razón es que existen funciones no estándar similares, como `memmem`. Sí usamos estas funciones en algunas ocasiones. Estas funciones no existen en `namespace std`.

Si escribe `std::memcpy` en lugar de `memcpy` en todas partes, entonces `memmem` sin `std::` resultará extraño.

No obstante, puede seguir usando `std::` si lo prefiere.

**3.** Usar funciones de C cuando las mismas están disponibles en la biblioteca estándar de C++.

Esto es aceptable si resulta más eficiente.

Por ejemplo, use `memcpy` en lugar de `std::copy` para copiar grandes bloques de memoria.

**4.** Argumentos de función en varias líneas.

Se permite cualquiera de los siguientes estilos de ajuste de línea:

```cpp
function(
  T1 x1,
  T2 x2)
```

```cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

```cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

```cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

```cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```