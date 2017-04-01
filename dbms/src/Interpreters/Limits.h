#pragma once

#include <Poco/Timespan.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <Interpreters/SettingsCommon.h>


namespace DB
{

/** Ограничения при выполнении запроса - часть настроек.
  * Используются, чтобы обеспечить более безопасное исполнение запросов из пользовательского интерфейса.
  * В основном, ограничения проверяются на каждый блок (а не на каждую строку). То есть, ограничения могут быть немного нарушены.
  * Почти все ограничения действуют только на SELECT-ы.
  * Почти все ограничения действуют на каждый поток по отдельности.
  */
struct Limits
{
    /** Перечисление ограничений: тип, имя, значение по-умолчанию.
      * По-умолчанию: всё не ограничено, кроме довольно слабых ограничений на глубину рекурсии и размер выражений.
      */

#define APPLY_FOR_LIMITS(M) \
    /** Ограничения на чтение из самых "глубоких" источников. \
      * То есть, только в самом глубоком подзапросе. \
      * При чтении с удалённого сервера, проверяется только на удалённом сервере. \
      */ \
    M(SettingUInt64, max_rows_to_read, 0) \
    M(SettingUInt64, max_bytes_to_read, 0) \
    M(SettingOverflowMode<false>, read_overflow_mode, OverflowMode::THROW) \
    \
    M(SettingUInt64, max_rows_to_group_by, 0) \
    M(SettingOverflowMode<true>, group_by_overflow_mode, OverflowMode::THROW) \
    M(SettingUInt64, max_bytes_before_external_group_by, 0) \
    \
    M(SettingUInt64, max_rows_to_sort, 0) \
    M(SettingUInt64, max_bytes_to_sort, 0) \
    M(SettingOverflowMode<false>, sort_overflow_mode, OverflowMode::THROW) \
    M(SettingUInt64, max_bytes_before_external_sort, 0) \
    \
    /** Ограничение на размер результата. \
      * Проверяются также для подзапросов и на удалённых серверах. \
      */ \
    M(SettingUInt64, max_result_rows, 0) \
    M(SettingUInt64, max_result_bytes, 0) \
    M(SettingOverflowMode<false>, result_overflow_mode, OverflowMode::THROW) \
    \
    /* TODO: Проверять также при слиянии и финализации агрегатных функций. */ \
    M(SettingSeconds, max_execution_time, 0) \
    M(SettingOverflowMode<false>, timeout_overflow_mode, OverflowMode::THROW) \
    \
    /** В строчках в секунду. */ \
    M(SettingUInt64, min_execution_speed, 0) \
    /** Проверять, что скорость не слишком низкая, после прошествия указанного времени. */ \
    M(SettingSeconds, timeout_before_checking_execution_speed, 0) \
    \
    M(SettingUInt64, max_columns_to_read, 0) \
    M(SettingUInt64, max_temporary_columns, 0) \
    M(SettingUInt64, max_temporary_non_const_columns, 0) \
    \
    M(SettingUInt64, max_subquery_depth, 100) \
    M(SettingUInt64, max_pipeline_depth, 1000) \
    M(SettingUInt64, max_ast_depth, 1000)        /** Проверяются не во время парсинга, */ \
    M(SettingUInt64, max_ast_elements, 50000)    /**  а уже после парсинга запроса. */ \
    \
    /** 0 - можно всё. 1 - только запросы на чтение. 2 - только запросы на чтение, а также изменение настроек, кроме настройки readonly. */ \
    M(SettingUInt64, readonly, 0) \
    \
    /** Ограничения для максимального размера множества, получающегося при выполнении секции IN. */ \
    M(SettingUInt64, max_rows_in_set, 0) \
    M(SettingUInt64, max_bytes_in_set, 0) \
    M(SettingOverflowMode<false>, set_overflow_mode, OverflowMode::THROW) \
    \
    /** Ограничения для максимального размера множества, получающегося при выполнении секции IN. */ \
    M(SettingUInt64, max_rows_in_join, 0) \
    M(SettingUInt64, max_bytes_in_join, 0) \
    M(SettingOverflowMode<false>, join_overflow_mode, OverflowMode::THROW) \
    \
    /** Ограничения для максимального размера передаваемой внешней таблицы, получающейся при выполнении секции GLOBAL IN/JOIN. */ \
    M(SettingUInt64, max_rows_to_transfer, 0) \
    M(SettingUInt64, max_bytes_to_transfer, 0) \
    M(SettingOverflowMode<false>, transfer_overflow_mode, OverflowMode::THROW) \
    \
    /** Ограничения для максимального размера запоминаемого состояния при выполнении DISTINCT. */ \
    M(SettingUInt64, max_rows_in_distinct, 0) \
    M(SettingUInt64, max_bytes_in_distinct, 0) \
    M(SettingOverflowMode<false>, distinct_overflow_mode, OverflowMode::THROW) \
    \
    /** Максимальное использование памяти при обработке запроса. 0 - не ограничено. */ \
    M(SettingUInt64, max_memory_usage, 0) /* На один запрос */ \
    /* Суммарно на одновременно выполняющиеся запросы одного пользователя */ \
    M(SettingUInt64, max_memory_usage_for_user, 0) \
    /* Суммарно на все одновременно выполняющиеся запросы */ \
    M(SettingUInt64, max_memory_usage_for_all_queries, 0) \
    \
    /** Максимальная скорость обмена данными по сети в байтах в секунду. 0 - не ограничена. */ \
    M(SettingUInt64, max_network_bandwidth, 0) \
    /** Максимальное количество байт на приём или передачу по сети, в рамках запроса. */ \
    M(SettingUInt64, max_network_bytes, 0) \

#define DECLARE(TYPE, NAME, DEFAULT) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_LIMITS(DECLARE)

#undef DECLARE

    /// Установить настройку по имени.
    bool trySet(const String & name, const Field & value)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT) \
        else if (name == #NAME) NAME.set(value);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

    /// Установить настройку по имени. Прочитать сериализованное в бинарном виде значение из буфера (для межсерверного взаимодействия).
    bool trySet(const String & name, ReadBuffer & buf)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT) \
        else if (name == #NAME) NAME.set(buf);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

    /// Пропустить сериализованное в бинарном виде значение из буфера.
    bool tryIgnore(const String & name, ReadBuffer & buf)
    {
    #define TRY_IGNORE(TYPE, NAME, DEFAULT) \
        else if (name == #NAME) decltype(NAME)(DEFAULT).set(buf);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_IGNORE)
        else
            return false;

        return true;

        #undef TRY_IGNORE
    }

    /** Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
      */
    bool trySet(const String & name, const String & value)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT) \
        else if (name == #NAME) NAME.set(value);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

private:
    friend struct Settings;

    /// Записать все настройки в буфер. (В отличие от соответствующего метода в Settings, пустая строка на конце не пишется).
    void serialize(WriteBuffer & buf) const
    {
    #define WRITE(TYPE, NAME, DEFAULT) \
        if (NAME.changed) \
        { \
            writeStringBinary(#NAME, buf); \
            NAME.write(buf); \
        }

        APPLY_FOR_LIMITS(WRITE)

    #undef WRITE
    }
};


}
