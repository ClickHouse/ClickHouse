const VersionHistory = ({ rows = [] }) => {
  const headers = ["Версия", "Значение по умолчанию", "Комментарий"];
  return (
    <Accordion title="История версий">
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