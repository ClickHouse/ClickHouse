/**
 * `VersionHistory` — accordion containing a 3-column table of when a setting
 * changed across ClickHouse versions.
 *
 * Mintlify equivalent of clickhouse-docs's `src/theme/VersionHistory`.
 *
 * Usage:
 *   import { VersionHistory } from "/snippets/components/VersionHistory/VersionHistory.jsx";
 *
 *   <VersionHistory rows={[
 *     { id: "row-1", items: [
 *       { label: "26.4" },
 *       { label: "0" },
 *       { label: "New setting" }
 *     ]},
 *   ]}/>
 *
 * Each row's `items` is expected to be `[Version, Default value, Comment]`.
 */
export const VersionHistory = ({ rows = [] }) => {
  const headers = ["Version", "Default value", "Comment"];
  return (
    <details>
      <summary>Version history</summary>
      <table>
        <thead>
          <tr>
            {headers.map((h) => (
              <th key={h}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={row.id ?? ri}>
              {(row.items ?? []).map((cell, ci) => (
                <td key={ci}>{cell?.label}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
};

export default VersionHistory;
