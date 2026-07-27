const VersionHistory = ({ rows = [] }) => {
  const headers = ["バージョン", "デフォルト値", "コメント"];
  return (
    <Accordion title="バージョン履歴">
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
    </Accordion>
  );
};
export default VersionHistory;