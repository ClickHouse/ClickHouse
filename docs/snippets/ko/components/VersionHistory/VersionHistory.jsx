export const VersionHistory = ({ rows = [] }) => {
  if (rows.length === 0) {
    return null;
  }

  const headers = ["버전", "기본값", "설명"];
  const border = "1px solid rgba(128, 128, 128, 0.3)";
  const cell = {
    border,
    padding: "0.25rem 0.5rem",
    textAlign: "start",
    verticalAlign: "top",
  };

  return (
    <details
      className="not-prose"
      style={{
        border,
        borderRadius: "0.5rem",
        margin: "0.5rem 0",
        padding: "0.5rem 0.75rem",
        fontSize: "0.8125rem",
        lineHeight: "1.125rem",
      }}
    >
      <summary style={{ cursor: "pointer", fontWeight: 600, opacity: 0.72 }}>
        버전 이력
      </summary>
      <table style={{ borderCollapse: "collapse", width: "100%", margin: "0.5rem 0 0" }}>
        <thead>
          <tr>
            {headers.map((header) => (
              <th key={header} style={{ ...cell, fontWeight: 600, opacity: 0.72 }}>
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, row_index) => (
            <tr key={row.id ?? row_index}>
              {(row.items ?? []).map((item, item_index) => (
                <td key={item_index} style={{ ...cell, overflowWrap: "anywhere" }}>
                  {item?.label}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
};

export default VersionHistory;
